"""Resumable loading of remote `requestsFromUrl` sources for `ApifyRequestList`.

The machinery in this module turns the remote entries of the standard Apify
`requestListSources` Actor input into a deterministic, deduplicated sequence of
`Request` objects that can be resumed across interruptions:

- Every remote source has a stable identity (`source_id`) and belongs to a source
  set generation (`generation_id`) persisted in the default key-value store.
- Sources are streamed in chunks through an incremental decompressor (gzip/deflate),
  split on complete line boundaries and parsed either as JSONL records or with the
  plain-text URL extraction regex.
- A source checkpoint only advances after a segment of records has been fully
  decoded, adjudicated by the global deduplication set and merged into the final
  request sequence.
- On resume, range-capable servers are contacted with `Range`/`If-Range`; changed
  content invalidates the checkpoint and triggers a full reload, while servers that
  ignore range requests are replayed from the start, verified against the stored
  prefix digest and skipped by the confirmed request identities.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import zlib
from contextlib import suppress
from copy import deepcopy
from dataclasses import dataclass
from http import HTTPStatus
from typing import TYPE_CHECKING, Annotated, Any, Literal
from uuid import uuid4

from impit import HTTPError as ImpitHTTPError
from impit import TransportError as ImpitTransportError
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from pydantic.alias_generators import to_camel

from crawlee import Request, service_locator
from crawlee._types import HttpMethod, JsonSerializable
from crawlee.events._types import Event, EventPersistStateData

if TYPE_CHECKING:
    from collections.abc import Awaitable

    from crawlee.http_clients import HttpClient, HttpResponse
    from crawlee.storages import KeyValueStore

logger = logging.getLogger(__name__)

URL_NO_COMMAS_REGEX = re.compile(
    r'https?:\/\/(www\.)?([^\W_]|[^\W_][-\w0-9@:%._+~#=]{0,254}[^\W_])\.[a-z]{2,63}(:\d{1,5})?(\/[-\w@:%+.~#?&/=()]*)?'
)

#: Minimum number of bytes needed to recognize a gzip/zlib stream header.
_MIN_HEADER_BYTES = 2
#: Number of fully decoded records accumulated before a parser segment is closed.
_SEGMENT_MAX_RECORDS = 100
#: Minimum number of entity bytes between two checkpoint writes of the same source.
_PERSIST_MIN_WIRE_BYTES = 256 * 1024
#: Maximum number of attempts (first try included) to download a single source.
_MAX_DOWNLOAD_ATTEMPTS = 4
_RETRY_BACKOFF_SECONDS = (0.0, 0.5, 1.0, 2.0)
#: Maximum time the PERSIST_STATE listener waits for in-flight segments to be committed.
_PERSIST_LISTENER_TIMEOUT_SECONDS = 15

_EMPTY_SHA256 = hashlib.sha256(b'').hexdigest()

_MANIFEST_KEY_TEMPLATE = 'SDK_REQUEST_LIST_SOURCES_STATE-{name}'
_SOURCE_KEY_TEMPLATE = 'SDK_REQUEST_LIST_SOURCE_STATE-{name}-{index}-{source_id}'
_SOURCE_ID_SHORT_LENGTH = 16

_CONTENT_RANGE_REGEX = re.compile(r'bytes\s+(?:(\d+)\s*-\s*\d*|\*)\s*/\s*(\d+|\*)', re.IGNORECASE)


class _ContentChangedError(Exception):
    """The resumed source entity no longer matches the checkpoint."""


class _RetryableDownloadError(Exception):
    """A source download failed in a potentially transient way and may be retried."""


_RETRYABLE_EXCEPTIONS = (
    _RetryableDownloadError,
    ImpitTransportError,
    ImpitHTTPError,
    OSError,
    ConnectionError,
    TimeoutError,
    asyncio.TimeoutError,
    EOFError,
    zlib.error,
)

# Queue sentinels shared with the merger.
_FATAL = object()
_DONE = object()


class _RequestDetails(BaseModel):
    """Request properties shared by all requests created from one input entry."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    method: HttpMethod = 'GET'
    payload: str = ''
    headers: Annotated[dict[str, str], Field(default_factory=dict)]
    user_data: Annotated[dict[str, JsonSerializable], Field(default_factory=dict)]


class _RequestsFromUrlInput(_RequestDetails):
    """A remote source entry (`requestsFromUrl`)."""

    requests_from_url: str


class _SimpleUrlInput(_RequestDetails):
    """An inline URL entry (`url`)."""

    url: str


class _LineRequestInput(_RequestDetails):
    """A single request specified on one JSONL line of a remote source."""

    url: str


url_input_adapter = TypeAdapter(list[_RequestsFromUrlInput | _SimpleUrlInput])


def _canonical_source(source: _RequestsFromUrlInput) -> str:
    """Return a deterministic canonical JSON representation of a remote source entry."""
    return json.dumps(
        source.model_dump(mode='json', by_alias=True),
        sort_keys=True,
        separators=(',', ':'),
        ensure_ascii=False,
    )


def _compute_source_id(index: int, canonical_entry: str) -> str:
    """Derive the stable identity of a remote source from its index and entry content."""
    digest = hashlib.sha256(f'{index}:{canonical_entry}'.encode())
    return digest.hexdigest()


def _compute_source_set_hash(canonical_entries: list[str]) -> str:
    """Hash the whole ordered set of remote source entries (order-sensitive)."""
    digest = hashlib.sha256()
    for index, canonical_entry in enumerate(canonical_entries):
        digest.update(f'{index}:{canonical_entry}\n'.encode())
    return digest.hexdigest()


class _WireSpan(BaseModel):
    """SHA-256 digest of a half-open entity byte range covered by a checkpoint."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    start: int
    end: int
    digest: str


class _SourceCheckpoint(BaseModel):
    """Persisted state of a single remote source.

    The checkpoint records the source identity, response validation metadata, the
    confirmed parsing boundary (`raw_offset` / `content_offset` with digests), the
    ordered identities of the confirmed requests and their full serialized form so
    that a completed source can be rebuilt without any HTTP traffic.
    """

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    source_id: str
    generation_id: str
    url: str
    encoding: str = 'identity'
    etag: str | None = None
    last_modified: str | None = None
    total_length: int | None = None
    raw_offset: int = 0
    content_offset: int = 0
    prefix_sha256: str = _EMPTY_SHA256
    wire_spans: Annotated[list[_WireSpan], Field(default_factory=list)]
    confirmed_keys: Annotated[list[str], Field(default_factory=list)]
    confirmed_requests: Annotated[list[dict[str, Any]], Field(default_factory=list)]
    completed: bool = False


class _CheckpointManifest(BaseModel):
    """Persisted generation manifest describing the whole ordered set of sources."""

    model_config = ConfigDict(populate_by_name=True, alias_generator=to_camel)

    version: int = 1
    generation_id: str
    source_set_hash: str
    source_ids: Annotated[list[str], Field(default_factory=list)]


class _CheckpointStore:
    """Key-value store wrapper for source checkpoints.

    When `name` is `None`, persistence is disabled and every operation is a no-op,
    so the unnamed code path never touches a key-value store.
    """

    def __init__(self, name: str | None) -> None:
        self._name = name
        self._enabled = name is not None
        self._kvs: KeyValueStore | None = None

    @property
    def enabled(self) -> bool:
        """Whether checkpoint persistence is active."""
        return self._enabled

    async def initialize(self) -> None:
        """Open the default key-value store when persistence is enabled."""
        if self._enabled:
            from crawlee.storages import KeyValueStore  # noqa: PLC0415

            self._kvs = await KeyValueStore.open()

    def manifest_key(self) -> str:
        """Return the KVS key of the manifest record."""
        return _MANIFEST_KEY_TEMPLATE.format(name=self._name)

    def source_key(self, index: int, source_id: str) -> str:
        """Return the KVS key of one source checkpoint record."""
        return _SOURCE_KEY_TEMPLATE.format(
            name=self._name,
            index=index,
            source_id=source_id[:_SOURCE_ID_SHORT_LENGTH],
        )

    async def load_manifest(self) -> _CheckpointManifest | None:
        """Load the manifest record, if any."""
        if self._kvs is None:
            return None
        data = await self._kvs.get_value(self.manifest_key())
        return _CheckpointManifest.model_validate(data) if data is not None else None

    async def save_manifest(self, manifest: _CheckpointManifest) -> None:
        """Persist the manifest record; raises to the caller on failure."""
        if self._kvs is not None:
            await self._kvs.set_value(
                self.manifest_key(),
                manifest.model_dump(mode='json', by_alias=True),
                'application/json',
            )

    async def load_source(self, key: str) -> _SourceCheckpoint | None:
        """Load one source checkpoint, if any."""
        if self._kvs is None:
            return None
        data = await self._kvs.get_value(key)
        return _SourceCheckpoint.model_validate(data) if data is not None else None

    async def save_source(self, key: str, checkpoint: _SourceCheckpoint) -> None:
        """Persist one source checkpoint; raises to the caller on failure."""
        if self._kvs is not None:
            await self._kvs.set_value(
                key,
                checkpoint.model_dump(mode='json', by_alias=True),
                'application/json',
            )

    async def delete_source(self, key: str) -> None:
        """Delete one source checkpoint (missing records are ignored)."""
        if self._kvs is not None:
            await self._kvs.set_value(key, None)


def _request_from_details(details: _RequestDetails, *, url: str) -> Request:
    """Construct a `Request` from entry-level details and an extracted URL."""
    return Request.from_url(
        url=url,
        method=details.method,
        payload=details.payload.encode('utf-8'),
        headers=details.headers,
        # Deep-copy so `Request.from_url` (which writes `__crawlee` into the dict) cannot corrupt
        # the shared input, and nested JSON values are not aliased across the requests.
        user_data=deepcopy(details.user_data),
    )


def _parse_jsonl_details(line: str, source: _RequestsFromUrlInput) -> _LineRequestInput | None:
    """Parse a single JSONL line into request details, or return `None` for regex fallback.

    A line can be a JSON string (the URL) or a JSON object with a required `url` field
    and optional `method`, `payload`, `headers` and `userData` fields that override the
    source-level defaults for this request.
    """
    try:
        value = json.loads(line)
    except (json.JSONDecodeError, ValueError):
        return None

    if isinstance(value, str):
        override: dict[str, Any] = {'url': value}
    elif isinstance(value, dict) and isinstance(value.get('url'), str):
        override = {key: value[key] for key in ('url', 'method', 'payload', 'headers', 'userData') if key in value}
    else:
        return None

    data: dict[str, Any] = {
        'url': source.requests_from_url,
        'method': source.method,
        'payload': source.payload,
        'headers': source.headers,
        'userData': source.user_data,
    }
    data.update(override)
    return _LineRequestInput.model_validate(data)


def _requests_from_line(line: bytes, source: _RequestsFromUrlInput) -> list[Request]:
    """Create all requests encoded on one complete content line.

    JSONL lines (a URL string or a request object) are parsed as such; every other
    line is scanned with the plain-text URL extraction regex.
    """
    text = line.decode('utf-8', errors='replace')
    stripped = text.strip()
    if stripped:
        details = _parse_jsonl_details(stripped, source)
        if details is not None:
            return [_request_from_details(details, url=details.url)]

    return [_request_from_details(source, url=match.group(0)) for match in URL_NO_COMMAS_REGEX.finditer(text)]


@dataclass
class _ParsedSegment:
    """A batch of records decoded up to a restart-safe boundary."""

    keys: list[str]
    requests: list[Request]
    end_raw: int
    end_content: int
    content_digest: str | None
    persist: bool
    persist_start_raw: int
    persist_wire_digest: str
    phase: Literal['verify', 'live']
    final: bool = False


class _LineStreamParser:
    """Turns raw entity chunks into request segments at complete-line boundaries.

    The parser transparently inflates gzip/deflate streams (after confirming the
    declared encoding with the leading bytes), splits content on newline bytes
    (which never occur inside multi-byte UTF-8 sequences) and decodes each complete
    line separately. Bytes belonging to an unfinished trailing line are never
    considered confirmed, so half lines and half multi-byte characters cannot
    produce requests or advance the boundary.
    """

    def __init__(
        self,
        source: _RequestsFromUrlInput,
        *,
        encoding: str,
        wire_start: int,
        content_start: int,
        from_zero: bool,
        verify_keys: list[str] | None = None,
        verify_content_offset: int | None = None,
        verify_wire_spans: list[_WireSpan] | None = None,
    ) -> None:
        self._source = source
        self._declared_encoding = encoding
        self._wire_abs = wire_start
        self._content_offset = content_start
        self._tail = bytearray()

        self._codec_resolved = False
        self._codec_header = bytearray()
        self._decompressor: Any = None  # zlib decompressor object (not exported in the type stubs)
        self._identity = False

        self._content_hash = hashlib.sha256() if from_zero else None
        self._wire_persist_start = wire_start
        self._wire_persist_hash = hashlib.sha256()
        self._force_persist = False

        self._pending_requests: list[Request] = []
        self._pending_keys: list[str] = []
        self._finished = False

        # Replay verification state. When replaying, segments are emitted in the
        # `verify` phase (and compared against the checkpoint) until exactly the
        # confirmed record count has been seen at the confirmed content boundary.
        # `verify_wire_spans` is a contiguous digest coverage of [0, content_offset).
        self._verify_keys = list(verify_keys) if verify_keys is not None else None
        self._verify_pos = 0
        self._verify_content_offset = verify_content_offset or 0
        self._verify_spans = list(verify_wire_spans) if verify_wire_spans else None
        self._span_idx = 0
        self._span_hasher: Any = hashlib.sha256() if self._verify_spans else None
        self._emitted_count = 0
        self.verified = self._verify_keys is None

    def force_next_persist(self) -> None:
        """Request that the next closed segment be made durable."""
        self._force_persist = True

    def acknowledge(self, end_raw: int) -> None:
        """Reset the persistence digest window after a checkpoint write succeeded."""
        self._wire_persist_start = end_raw
        self._wire_persist_hash = hashlib.sha256()
        self._force_persist = False

    @staticmethod
    def _build_decompressor(chunk: bytes, declared_encoding: str) -> tuple[Any, bytes]:
        if declared_encoding == 'gzip' and chunk[:2] == b'\x1f\x8b':
            decompressor = zlib.decompressobj(16 + zlib.MAX_WBITS)
            return decompressor, decompressor.decompress(chunk)

        if declared_encoding == 'deflate':
            # `deflate` is usually zlib-wrapped, but some servers send raw deflate.
            for wbits in (zlib.MAX_WBITS, -zlib.MAX_WBITS):
                decompressor = zlib.decompressobj(wbits)
                try:
                    inflated = decompressor.decompress(chunk)
                except zlib.error:
                    continue
                return decompressor, inflated

        return None, chunk

    def feed(self, raw: bytes) -> list[_ParsedSegment]:
        """Consume one entity chunk and return the segments closed by it."""
        if not raw:
            return []

        self._wire_abs += len(raw)

        if not self._codec_resolved:
            # Wait for at least two header bytes so that gzip magic / the zlib
            # header can be recognized when chunks arrive one byte at a time.
            self._codec_header.extend(raw)
            if self._declared_encoding != 'identity' and len(self._codec_header) < _MIN_HEADER_BYTES:
                return []
            first = bytes(self._codec_header)
            self._codec_header.clear()
            self._decompressor, data = self._build_decompressor(first, self._declared_encoding)
            self._identity = self._decompressor is None
            self._codec_resolved = True
        elif self._decompressor is not None:
            data = self._decompressor.decompress(raw)
        else:
            data = raw

        # For compressed entities wire bytes cannot be mapped to line boundaries;
        # the persistence digest is therefore tracked on fed chunks. For identity
        # entities only bytes of complete lines are confirmed, so hashing follows
        # line extraction instead.
        if self._decompressor is not None:
            self._wire_persist_hash.update(raw)

        return self._ingest_content(data)

    def finish(self) -> list[_ParsedSegment]:
        """Flush decompressor buffers and the trailing line and close the parser."""
        if self._finished:
            return []
        self._finished = True

        segments: list[_ParsedSegment] = []
        if not self._codec_resolved and self._codec_header:
            # The stream ended before enough header bytes arrived - fall back to identity.
            first = bytes(self._codec_header)
            self._codec_header.clear()
            self._decompressor, data = self._build_decompressor(first, 'identity')
            self._identity = True
            self._codec_resolved = True
            segments.extend(self._ingest_content(data))
        elif self._decompressor is not None:
            segments.extend(self._ingest_content(self._decompressor.flush()))

        if self._tail:
            line = bytes(self._tail)
            self._tail.clear()
            if self._content_hash is not None:
                self._content_hash.update(line)
            if self._identity:
                self._wire_persist_hash.update(line)
            self._content_offset += len(line)
            self._consume_span_bytes(line)
            self._append_requests(line)

        if not self.verified:
            if self._emitted_count != len(self._verify_keys or []):
                raise _ContentChangedError(
                    f'Replayed source content does not match the checkpoint: expected '
                    f'{len(self._verify_keys or [])} confirmed requests, got {self._emitted_count}'
                )
            if self._pending_keys:
                segments.append(self._flush(verify_phase=True))
            self._finalize_verification()

        segments.append(self._flush(verify_phase=False, final=True))
        return segments

    def _append_requests(self, line: bytes) -> None:
        for request in _requests_from_line(line, self._source):
            self._pending_requests.append(request)
            self._pending_keys.append(request.unique_key)
            self._emitted_count += 1

    def _ingest_content(self, data: bytes) -> list[_ParsedSegment]:
        segments: list[_ParsedSegment] = []
        self._tail.extend(data)

        while (newline_index := self._tail.find(b'\n')) >= 0:
            line = bytes(self._tail[: newline_index + 1])
            del self._tail[: newline_index + 1]
            body = line[:-2] if line.endswith(b'\r\n') else line[:-1]

            if self._content_hash is not None:
                self._content_hash.update(line)
            if self._identity:
                self._wire_persist_hash.update(line)
            self._content_offset += len(line)
            self._consume_span_bytes(line)
            self._append_requests(body)

            expected_count = len(self._verify_keys or [])
            if not self.verified and self._emitted_count > expected_count:
                raise _ContentChangedError('Replayed source contains more requests than the checkpoint')

            at_cut = not self.verified and self._emitted_count == expected_count
            if len(self._pending_keys) >= _SEGMENT_MAX_RECORDS or at_cut:
                segments.append(self._flush(verify_phase=not self.verified))
                if at_cut:
                    self._finalize_verification()

        return segments

    def _consume_span_bytes(self, line: bytes) -> None:
        # Span boundaries are complete-line boundaries, so each line belongs to at
        # most one span; at most one span can be closed by a single line.
        if self._verify_spans is None or self._span_idx >= len(self._verify_spans):
            return
        target = self._verify_spans[self._span_idx]
        if self._content_offset < target.start:
            return
        if self._span_hasher is None:
            self._span_hasher = hashlib.sha256()
        self._span_hasher.update(line)
        if self._content_offset < target.end:
            return

        if self._content_offset != target.end or self._span_hasher.hexdigest() != target.digest:
            raise _ContentChangedError(
                f'Replayed prefix digest mismatch at bytes {target.start}-{target.end}, the source content changed'
            )
        self._span_idx += 1
        self._span_hasher = hashlib.sha256()

    def _finalize_verification(self) -> None:
        if self._content_offset != self._verify_content_offset:
            raise _ContentChangedError(
                f'Replayed boundary mismatch: expected {self._verify_content_offset} content bytes, '
                f'got {self._content_offset}'
            )
        if self._verify_spans is not None and self._span_idx != len(self._verify_spans):
            raise _ContentChangedError('Replayed prefix range digests do not cover the confirmed boundary')
        if self._verify_pos != len(self._verify_keys or []):
            raise _ContentChangedError('Replayed request identities do not cover the confirmed boundary')
        self.verified = True

    def _wire_boundary(self) -> int:
        # For identity entities content bytes are entity bytes; for compressed
        # entities only the count of fed chunks is available (range resume is not
        # attempted for compressed entities anyway).
        if self._decompressor is None:
            return self._content_offset
        return self._wire_abs

    def _bytes_since_persist(self) -> int:
        return self._wire_boundary() - self._wire_persist_start

    def _flush(self, *, verify_phase: bool, final: bool = False) -> _ParsedSegment:
        if verify_phase:
            expected = self._verify_keys or []
            observed = self._pending_keys
            if observed != expected[self._verify_pos : self._verify_pos + len(observed)]:
                raise _ContentChangedError('Replayed request identities do not match the confirmed keys')
            self._verify_pos += len(observed)

        persist = (not verify_phase) and (
            final or self._force_persist or self._bytes_since_persist() >= _PERSIST_MIN_WIRE_BYTES
        )
        segment = _ParsedSegment(
            keys=self._pending_keys,
            requests=self._pending_requests,
            end_raw=self._wire_boundary(),
            end_content=self._content_offset,
            content_digest=self._content_hash.copy().hexdigest() if self._content_hash is not None else None,
            persist=persist,
            persist_start_raw=self._wire_persist_start,
            persist_wire_digest=self._wire_persist_hash.copy().hexdigest(),
            phase='verify' if verify_phase else 'live',
            final=final,
        )
        self._pending_keys = []
        self._pending_requests = []
        return segment


@dataclass
class _Plan:
    """Runner plan delivered to the merger before any live segment."""

    kind: Literal['fresh', 'resume', 'replay', 'completed']
    checkpoint: _SourceCheckpoint


@dataclass
class _SegmentEvent:
    """A parser segment delivered to the merger together with its ack future."""

    segment: _ParsedSegment
    ack: asyncio.Future[bool]


def _detect_encoding(content_encoding: str | None) -> str:
    """Map a `Content-Encoding` header value to one of the supported encodings."""
    value = (content_encoding or '').strip().lower()
    if not value or value == 'identity':
        return 'identity'
    if 'gzip' in value:
        return 'gzip'
    if 'deflate' in value:
        return 'deflate'
    # Unknown encodings (e.g. br) are normally already transparently decoded by the HTTP client.
    logger.debug('Unsupported Content-Encoding %r, treating the entity as identity bytes', value)
    return 'identity'


def _parse_int_header(value: str | None) -> int | None:
    if value is None:
        return None
    with suppress(ValueError):
        return int(value)
    return None


def _parse_content_range(value: str | None) -> tuple[int | None, int | None] | None:
    """Parse a `Content-Range` header into `(start, total)`.

    Both `bytes start-end/total` and the unsatisfied-range form `bytes */total` are
    accepted; either component may be `None` (unknown / `*`).
    """
    if value is None:
        return None
    match = _CONTENT_RANGE_REGEX.search(value)
    if match is None:
        return None
    start = int(match.group(1)) if match.group(1) is not None else None
    total: int | None = None if match.group(2) == '*' else int(match.group(2))
    return start, total


class _SourceRunner:
    """Downloads, verifies and parses one remote source concurrently.

    The runner communicates with the deterministic merger through a small ordered
    queue: it first publishes its plan, then streams parser segments (acknowledged
    by the merger after each commit), terminated by a done sentinel. Fatal errors
    are captured and published as a fatal sentinel so that the merger can raise
    them at the deterministic merge point.
    """

    def __init__(
        self,
        *,
        index: int,
        source: _RequestsFromUrlInput,
        checkpoint: _SourceCheckpoint | None,
        store: _CheckpointStore,
        source_key: str,
        source_id: str,
        generation_id: str,
        http_client: HttpClient,
    ) -> None:
        self._index = index
        self._source = source
        self._store = store
        self._source_key = source_key
        self._source_id = source_id
        self._generation_id = generation_id
        self._http_client = http_client

        self.checkpoint = checkpoint
        self.queue: asyncio.Queue[Any] = asyncio.Queue(maxsize=8)
        self.fatal: BaseException | None = None

        self._parser: _LineStreamParser | None = None
        self._done = asyncio.Event()
        self._drain_future: asyncio.Future[None] | None = None
        self._plan_emitted = False
        self._force_persist_requested = False

    @property
    def source_key(self) -> str:
        """The KVS key of this source checkpoint."""
        return self._source_key

    def request_force_persist(self) -> Awaitable[Any]:
        """Ask the runner to make the next closed segment durable and return a waiter."""
        if self._done.is_set() or self.fatal is not None:
            return asyncio.sleep(0)

        if self._drain_future is None or self._drain_future.done():
            self._drain_future = asyncio.get_running_loop().create_future()
        self._force_persist_requested = True
        if self._parser is not None:
            self._parser.force_next_persist()
        return self._drain_future

    async def run(self) -> None:
        """Task entry point; never raises, failures are published via the queue."""
        try:
            await self._run_until_done()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.fatal = exc
            with suppress(asyncio.CancelledError):
                await self.queue.put(_FATAL)
        finally:
            self._done.set()
            if self._drain_future is not None and not self._drain_future.done():
                self._drain_future.set_result(None)

    async def _run_until_done(self) -> None:
        if self.checkpoint is not None and self.checkpoint.completed:
            await self.queue.put(_Plan(kind='completed', checkpoint=self.checkpoint))
            self._plan_emitted = True
            await self.queue.put(_DONE)
            return

        last_error: Exception | None = None
        reload_forced = False
        for attempt in range(_MAX_DOWNLOAD_ATTEMPTS):
            try:
                await self._attempt(reload_forced=reload_forced)
            except _ContentChangedError:
                logger.warning(
                    'Remote source %d (%s) changed since its checkpoint, discarding the checkpoint and reloading',
                    self._index,
                    self._source.requests_from_url,
                )
                if self.checkpoint is not None:
                    await self._store.delete_source(self._source_key)
                self.checkpoint = None
                reload_forced = True
                if self._plan_emitted:
                    # Segments of the old entity were already merged; a fresh run is required.
                    raise
            except asyncio.CancelledError:
                raise
            except _RETRYABLE_EXCEPTIONS as exc:
                last_error = exc
                delay = _RETRY_BACKOFF_SECONDS[min(attempt, len(_RETRY_BACKOFF_SECONDS) - 1)]
                logger.debug(
                    'Attempt %d to download remote source %d failed: %s; retrying in %.1fs',
                    attempt + 1,
                    self._index,
                    exc,
                    delay,
                )
                await asyncio.sleep(delay)
            else:
                return

        if last_error is not None:
            raise last_error
        msg = f'Failed to download remote source {self._index} after {_MAX_DOWNLOAD_ATTEMPTS} attempts'
        raise _RetryableDownloadError(msg)

    async def _attempt(self, *, reload_forced: bool) -> None:
        checkpoint = self.checkpoint
        resuming = checkpoint is not None and checkpoint.raw_offset > 0 and not reload_forced

        headers: dict[str, str] = {}
        if resuming and checkpoint is not None:
            headers['Range'] = f'bytes={checkpoint.raw_offset}-'
            # Strong ETags only; weak validators are not allowed in If-Range.
            if checkpoint.etag and not checkpoint.etag.startswith('W/'):
                headers['If-Range'] = checkpoint.etag
            elif checkpoint.last_modified:
                headers['If-Range'] = checkpoint.last_modified

        url = checkpoint.url if checkpoint is not None else self._source.requests_from_url
        replay_without_range = False

        async with self._http_client.stream(url, method='GET', headers=headers or None) as response:
            acceptable = (HTTPStatus.OK, HTTPStatus.PARTIAL_CONTENT, HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE)
            if response.status_code not in acceptable:
                raise _RetryableDownloadError(f'Unexpected status code {response.status_code} from {url}')

            if (
                resuming
                and response.status_code == HTTPStatus.REQUESTED_RANGE_NOT_SATISFIABLE
                and checkpoint is not None
            ):
                if self._unsatisfied_range_means_changed(response, checkpoint):
                    raise _ContentChangedError(url)
                if checkpoint.encoding != 'identity':
                    # Compressed raw offsets cannot be aligned with a 416 boundary;
                    # replay the entity from the start instead of trusting an empty tail.
                    replay_without_range = True
                else:
                    # The confirmed boundary reaches the entity end and the content is
                    # still the same; there is no suffix left, only finalize.
                    parser = self._make_resume_parser(checkpoint)
                    self._parser = parser
                    await self._drive_finalized(parser, checkpoint)
            elif resuming and response.status_code == HTTPStatus.PARTIAL_CONTENT and checkpoint is not None:
                if self._can_resume(response, checkpoint):
                    self._parser = self._make_parser(response, 'resume', checkpoint)
                    await self._drive(response, self._parser, 'resume', checkpoint)
                else:
                    replay_without_range = True
            elif resuming and checkpoint is not None:
                if self._response_changed(response, checkpoint):
                    raise _ContentChangedError(url)
                self._parser = self._make_parser(response, 'replay', checkpoint)
                await self._drive(response, self._parser, 'replay', checkpoint)
            else:
                fresh = self._capture_fresh_checkpoint(response)
                self.checkpoint = fresh
                self._parser = self._make_parser(response, 'fresh', fresh)
                await self._drive(response, self._parser, 'fresh', fresh)

        if replay_without_range and checkpoint is not None:
            await self._attempt_replay(checkpoint)

    async def _attempt_replay(self, checkpoint: _SourceCheckpoint) -> None:
        """Re-open the source without a Range header and replay it from the start."""
        async with self._http_client.stream(checkpoint.url, method='GET') as response:
            if response.status_code != HTTPStatus.OK:
                raise _RetryableDownloadError(
                    f'Unexpected status code {response.status_code} when replaying {checkpoint.url}'
                )
            if self._response_changed(response, checkpoint):
                raise _ContentChangedError(checkpoint.url)
            parser = self._make_parser(response, 'replay', checkpoint)
            self._parser = parser
            await self._drive(response, parser, 'replay', checkpoint)

    @staticmethod
    def _validators_changed(response: HttpResponse, checkpoint: _SourceCheckpoint) -> bool:
        """Whether strong-ish response validators disagree with the checkpoint."""
        etag = response.headers.get('etag')
        if checkpoint.etag and etag and etag != checkpoint.etag:
            return True
        last_modified = response.headers.get('last-modified')
        return bool(checkpoint.last_modified and last_modified and last_modified != checkpoint.last_modified)

    @classmethod
    def _unsatisfied_range_means_changed(
        cls,
        response: HttpResponse,
        checkpoint: _SourceCheckpoint,
    ) -> bool:
        """Decide whether a 416 means the entity changed instead of a zero-length suffix."""
        if cls._validators_changed(response, checkpoint):
            return True
        parsed_range = _parse_content_range(response.headers.get('content-range'))
        current_total = parsed_range[1] if parsed_range is not None else None
        if current_total is not None:
            # Only a boundary exactly at the entity end proves an empty suffix.
            return current_total != checkpoint.raw_offset
        # Without a current length and without validator agreement the boundary cannot
        # be proven still valid - treat the entity as changed and reload from scratch.
        return True

    @staticmethod
    def _can_resume(response: HttpResponse, checkpoint: _SourceCheckpoint) -> bool:
        parsed_range = _parse_content_range(response.headers.get('content-range'))
        if parsed_range is None or parsed_range[0] != checkpoint.raw_offset:
            return False
        if _detect_encoding(response.headers.get('content-encoding')) != 'identity':
            return False
        total = parsed_range[1]
        return total is None or total >= checkpoint.raw_offset

    @classmethod
    def _response_changed(cls, response: HttpResponse, checkpoint: _SourceCheckpoint) -> bool:
        if cls._validators_changed(response, checkpoint):
            return True
        # The length comparison is only meaningful for identity entities: a compressed
        # Content-Length counts encoded bytes while the checkpoint boundary counts
        # decoded content (and a transparent client may keep the encoded header).
        if _detect_encoding(response.headers.get('content-encoding')) != 'identity':
            return False
        content_length = _parse_int_header(response.headers.get('content-length'))
        return content_length is not None and content_length < checkpoint.raw_offset

    def _capture_fresh_checkpoint(self, response: HttpResponse) -> _SourceCheckpoint:
        return _SourceCheckpoint(
            source_id=self._source_id,
            generation_id=self._generation_id,
            url=self._source.requests_from_url,
            encoding=_detect_encoding(response.headers.get('content-encoding')),
            etag=response.headers.get('etag'),
            last_modified=response.headers.get('last-modified'),
            total_length=_parse_int_header(response.headers.get('content-length')),
        )

    def _make_parser(
        self,
        response: HttpResponse,
        mode: Literal['fresh', 'resume', 'replay'],
        checkpoint: _SourceCheckpoint,
    ) -> _LineStreamParser:
        encoding = _detect_encoding(response.headers.get('content-encoding'))

        if mode == 'resume':
            return _LineStreamParser(
                self._source,
                encoding='identity',
                wire_start=checkpoint.raw_offset,
                content_start=checkpoint.content_offset,
                from_zero=False,
            )

        if mode == 'replay':
            return _LineStreamParser(
                self._source,
                encoding=encoding,
                wire_start=0,
                content_start=0,
                from_zero=True,
                verify_keys=checkpoint.confirmed_keys,
                verify_content_offset=checkpoint.content_offset,
                verify_wire_spans=self._replay_spans(checkpoint),
            )

        return _LineStreamParser(
            self._source,
            encoding=encoding,
            wire_start=0,
            content_start=0,
            from_zero=True,
        )

    @staticmethod
    def _replay_spans(checkpoint: _SourceCheckpoint) -> list[_WireSpan] | None:
        """Build the contiguous digest coverage of the confirmed prefix for replay."""
        spans = list(checkpoint.wire_spans)
        if checkpoint.prefix_sha256 and checkpoint.content_offset > 0:
            first_start = spans[0].start if spans else checkpoint.content_offset
            if first_start > 0:
                spans.insert(
                    0,
                    _WireSpan(start=0, end=first_start, digest=checkpoint.prefix_sha256),
                )
        return spans or None

    def _make_resume_parser(self, checkpoint: _SourceCheckpoint) -> _LineStreamParser:
        """Build the parser used when the confirmed boundary already ends the entity."""
        return _LineStreamParser(
            self._source,
            encoding='identity',
            wire_start=checkpoint.raw_offset,
            content_start=checkpoint.content_offset,
            from_zero=False,
        )

    async def _drive_finalized(self, parser: _LineStreamParser, checkpoint: _SourceCheckpoint) -> None:
        """Publish a resume plan and a final empty segment without reading a body."""
        if not self._plan_emitted:
            await self.queue.put(_Plan(kind='resume', checkpoint=checkpoint))
            self._plan_emitted = True
        for segment in parser.finish():
            await self._emit_segment(segment)
        await self.queue.put(_DONE)

    async def _drive(
        self,
        response: HttpResponse,
        parser: _LineStreamParser,
        mode: Literal['fresh', 'resume', 'replay'],
        checkpoint: _SourceCheckpoint,
    ) -> None:
        """Stream one HTTP response through the parser and publish its segments."""
        if self._force_persist_requested:
            # Carry a persist request over a download retry (the old parser is gone).
            parser.force_next_persist()

        stream_iter = response.read_stream().__aiter__()
        buffered: list[_ParsedSegment] = []
        ended = False

        if mode == 'replay':
            buffered, ended = await self._verify_prefix(stream_iter, parser)

        if not self._plan_emitted:
            # The plan is published exactly once; download retries resume from the
            # in-memory checkpoint without re-injecting any prefix.
            await self.queue.put(_Plan(kind=mode, checkpoint=checkpoint))
            self._plan_emitted = True
        for segment in buffered:
            await self._emit_segment(segment)

        if not ended:
            while True:
                try:
                    chunk = await stream_iter.__anext__()
                except StopAsyncIteration:
                    break
                for segment in parser.feed(chunk):
                    await self._emit_segment(segment)

            for segment in parser.finish():
                await self._emit_segment(segment)

        await self.queue.put(_DONE)

    async def _verify_prefix(
        self,
        stream_iter: Any,
        parser: _LineStreamParser,
    ) -> tuple[list[_ParsedSegment], bool]:
        """Consume the confirmed prefix before publishing the replay plan.

        Returns the live segments already decoded past the boundary and whether the
        stream ended while verifying.
        """
        buffered: list[_ParsedSegment] = []
        while not parser.verified:
            try:
                chunk = await stream_iter.__anext__()
            except StopAsyncIteration:
                tail = parser.finish()
                if not parser.verified:
                    raise _ContentChangedError('Stream ended before the confirmed boundary could be verified') from None
                return [segment for segment in tail if segment.phase == 'live'], True

            for segment in parser.feed(chunk):
                if segment.phase == 'live':
                    buffered.extend([segment])

        return buffered, False

    async def _emit_segment(self, segment: _ParsedSegment) -> None:
        ack: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
        await self.queue.put(_SegmentEvent(segment=segment, ack=ack))
        persisted = await ack
        if segment.persist and persisted and self._parser is not None:
            self._parser.acknowledge(segment.end_raw)
            self._force_persist_requested = False
        if segment.persist and self._drain_future is not None and not self._drain_future.done():
            self._drain_future.set_result(None)


def _admit(request: Request, seen: set[str], requests: list[Request]) -> None:
    """Append a request to the final sequence if its identity wins global dedup."""
    if request.unique_key not in seen:
        seen.add(request.unique_key)
        requests.append(request)


async def _next_event(runner: _SourceRunner) -> Any:
    """Read one queue item from a runner, surfacing its captured fatal error."""
    item = await runner.queue.get()
    if item is not _FATAL:
        return item
    if runner.fatal is not None:
        raise runner.fatal
    msg = f'Remote source runner {runner.source_key} failed without an error attached'
    raise RuntimeError(msg)


async def _commit_segment(
    runner: _SourceRunner,
    plan: _Plan,
    segment: _ParsedSegment,
    *,
    store: _CheckpointStore,
    requests: list[Request],
    seen: set[str],
) -> bool:
    """Adjudicate and merge one segment and persist the advanced checkpoint."""
    checkpoint = plan.checkpoint

    accepted: list[Request] = []
    for request in segment.requests:
        if request.unique_key not in seen:
            seen.add(request.unique_key)
            accepted.append(request)
            requests.append(request)

    checkpoint.confirmed_keys.extend(segment.keys)
    checkpoint.confirmed_requests.extend(request.model_dump(mode='json', by_alias=True) for request in accepted)
    checkpoint.raw_offset = segment.end_raw
    checkpoint.content_offset = segment.end_content

    # Segments decoded from the entity start carry the full prefix digest.
    # Suffix segments (206 resume) can only contribute a wire range span.
    if segment.content_digest is not None:
        checkpoint.prefix_sha256 = segment.content_digest
        checkpoint.wire_spans = []

    if not segment.persist:
        return True

    added_span = False
    if segment.content_digest is None and segment.end_raw > segment.persist_start_raw:
        checkpoint.wire_spans.append(
            _WireSpan(
                start=segment.persist_start_raw,
                end=segment.end_raw,
                digest=segment.persist_wire_digest,
            )
        )
        added_span = True
    if segment.final:
        checkpoint.completed = True
    try:
        await store.save_source(runner.source_key, checkpoint)
    except Exception as exc:
        logger.warning('Failed to persist source checkpoint, keeping the previous one: %s', exc)
        if added_span:
            checkpoint.wire_spans.pop()
        if segment.final:
            checkpoint.completed = False
        return False
    return True


async def _consume_sources(
    runners: list[_SourceRunner],
    store: _CheckpointStore,
    requests: list[Request],
    seen: set[str],
) -> None:
    """Merge source results into the final sequence in deterministic source order."""
    for runner in runners:
        plan: _Plan = await _next_event(runner)

        if plan.kind in ('resume', 'replay', 'completed'):
            for serialized in plan.checkpoint.confirmed_requests:
                _admit(Request.model_validate(serialized), seen, requests)

        if plan.kind == 'completed':
            await _next_event(runner)  # The closing done sentinel.
            continue

        while True:
            item = await _next_event(runner)
            if item is _DONE:
                break
            event: _SegmentEvent = item
            persisted = await _commit_segment(runner, plan, event.segment, store=store, requests=requests, seen=seen)
            event.ack.set_result(persisted)


async def append_remote_requests(
    requests: list[Request],
    seen: set[str],
    *,
    name: str | None,
    remote_inputs: list[_RequestsFromUrlInput],
    http_client: HttpClient,
) -> None:
    """Stream all remote sources and append their deduplicated requests in order.

    `requests` and `seen` already contain the inline requests; remote requests are
    appended after them, with remote sources merged in their input index order
    regardless of their concurrent download completion order.
    """
    if not remote_inputs:
        return

    store = _CheckpointStore(name)
    await store.initialize()

    canonical_entries = [_canonical_source(source) for source in remote_inputs]
    source_ids = [_compute_source_id(index, entry) for index, entry in enumerate(canonical_entries)]
    source_set_hash = _compute_source_set_hash(canonical_entries)

    manifest = await store.load_manifest()
    if manifest is None:
        generation_id = uuid4().hex
        manifest = _CheckpointManifest(
            generation_id=generation_id,
            source_set_hash=source_set_hash,
            source_ids=source_ids,
        )
        await store.save_manifest(manifest)
    elif manifest.source_set_hash != source_set_hash:
        logger.info('Remote source set changed, discarding checkpoints of the previous generation')
        for index, old_source_id in enumerate(manifest.source_ids):
            await store.delete_source(store.source_key(index, old_source_id))
        generation_id = uuid4().hex
        manifest = _CheckpointManifest(
            generation_id=generation_id,
            source_set_hash=source_set_hash,
            source_ids=source_ids,
        )
        await store.save_manifest(manifest)
    else:
        generation_id = manifest.generation_id

    runners: list[_SourceRunner] = []
    for index, (source, source_id) in enumerate(zip(remote_inputs, source_ids, strict=True)):
        source_key = store.source_key(index, source_id)
        checkpoint = await store.load_source(source_key)
        runners.append(
            _SourceRunner(
                index=index,
                source=source,
                checkpoint=checkpoint,
                store=store,
                source_key=source_key,
                source_id=source_id,
                generation_id=generation_id,
                http_client=http_client,
            )
        )

    async def persist_state_listener(_event_data: EventPersistStateData | None = None) -> None:
        waiters = [runner.request_force_persist() for runner in runners]
        await asyncio.wait_for(
            asyncio.gather(*waiters, return_exceptions=True),
            timeout=_PERSIST_LISTENER_TIMEOUT_SECONDS,
        )

    event_manager = service_locator.get_event_manager()
    if store.enabled:
        event_manager.on(event=Event.PERSIST_STATE, listener=persist_state_listener)

    try:
        async with asyncio.TaskGroup() as task_group:
            for runner in runners:
                task_group.create_task(runner.run())
            await _consume_sources(runners, store, requests, seen)
    finally:
        if store.enabled:
            event_manager.off(event=Event.PERSIST_STATE, listener=persist_state_listener)
            # Best-effort durability for committed but unpersisted boundaries when
            # loading fails for any reason (e.g. another source is fatal, cancellation).
            with suppress(BaseException):
                await _flush_pending_checkpoints(runners, store)


async def _flush_pending_checkpoints(runners: list[_SourceRunner], store: _CheckpointStore) -> None:
    """Persist every committed, not yet completed source checkpoint once more."""

    async def _flush_one(runner: _SourceRunner) -> None:
        checkpoint = runner.checkpoint
        if checkpoint is None or checkpoint.completed:
            return
        try:
            await store.save_source(runner.source_key, checkpoint)
        except Exception as exc:
            logger.warning('Best-effort checkpoint flush failed for %s: %s', runner.source_key, exc)

    await asyncio.gather(*(_flush_one(runner) for runner in runners), return_exceptions=True)
