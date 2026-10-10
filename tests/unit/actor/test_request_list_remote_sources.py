from __future__ import annotations

import asyncio
import gzip
import hashlib
import zlib
from typing import TYPE_CHECKING, Any

import pytest
from pydantic import ValidationError
from werkzeug import Request, Response

from crawlee import service_locator
from crawlee.events._types import Event, EventPersistStateData
from crawlee.http_clients import ImpitHttpClient
from crawlee.storages import KeyValueStore

from apify.request_loaders import ApifyRequestList
from apify.request_loaders import _remote_sources as rs

if TYPE_CHECKING:
    from pytest_httpserver import HTTPServer

    from crawlee._types import HttpMethod

pytestmark = pytest.mark.xdist_group('remote-request-list')


LINES = [f'http://www.line-{i:03d}.com' for i in range(60)]
BODY = ('\n'.join(LINES) + '\n').encode()


@pytest.fixture(autouse=True)
def _small_checkpoint_units(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rs, '_PERSIST_MIN_WIRE_BYTES', 128)
    monkeypatch.setattr(rs, '_SEGMENT_MAX_RECORDS', 5)
    monkeypatch.setattr(rs, '_RETRY_BACKOFF_SECONDS', (0.0, 0.0, 0.0, 0.0))


async def _drain_urls(request_list: ApifyRequestList) -> list[str]:
    urls: list[str] = []
    while request := await request_list.fetch_next_request():
        urls.append(request.url)
    return urls


def _range_response(request: Request, body: bytes, *, etag: str = '"v1"') -> Response:
    range_header = request.headers.get('Range')
    if range_header:
        start = int(range_header.removeprefix('bytes=').split('-')[0])
        if start >= len(body):
            return Response(status=416)
        return Response(
            body[start:],
            status=206,
            headers={
                'Content-Range': f'bytes {start}-{len(body) - 1}/{len(body)}',
                'ETag': etag,
                'Accept-Ranges': 'bytes',
            },
        )
    return Response(body, status=200, headers={'ETag': etag, 'Accept-Ranges': 'bytes'})


async def _seed_generation(name: str, source_inputs: list[dict[str, Any]]) -> tuple[str, str, str]:
    """Create a manifest + one source checkpoint key directly in the default KVS.

    Returns the generation id, source checkpoint key and full source id.
    """
    adapter_inputs = rs.url_input_adapter.validate_python(source_inputs)
    remote_inputs = [item for item in adapter_inputs if isinstance(item, rs._RequestsFromUrlInput)]
    source = remote_inputs[0]
    canonical = rs._canonical_source(source)
    source_id = rs._compute_source_id(0, canonical)
    generation_id = 'test-generation'

    store = rs._CheckpointStore(name)
    manifest = rs._CheckpointManifest(
        generation_id=generation_id,
        source_set_hash=rs._compute_source_set_hash([canonical]),
        source_ids=[source_id],
    )
    kvs = await KeyValueStore.open()
    await kvs.set_value(
        store.manifest_key(),
        manifest.model_dump(mode='json', by_alias=True),
        'application/json',
    )
    return generation_id, store.source_key(0, source_id), source_id


def test_source_identities_are_stable_and_order_sensitive() -> None:
    inputs = rs.url_input_adapter.validate_python(
        [
            {'requestsFromUrl': 'http://example.com/a.txt', 'method': 'GET'},
            {'requestsFromUrl': 'http://example.com/b.txt'},
        ]
    )
    remotes = [item for item in inputs if isinstance(item, rs._RequestsFromUrlInput)]
    canonical = [rs._canonical_source(item) for item in remotes]
    ids = [rs._compute_source_id(i, c) for i, c in enumerate(canonical)]

    assert rs._compute_source_id(0, canonical[0]) == ids[0]
    assert ids[0] != ids[1]

    reordered = [canonical[1], canonical[0]]
    assert rs._compute_source_set_hash(canonical) != rs._compute_source_set_hash(reordered)

    modified_inputs = rs.url_input_adapter.validate_python(
        [{'requestsFromUrl': 'http://example.com/a.txt', 'method': 'POST'}]
    )
    modified_remote = modified_inputs[0]
    assert isinstance(modified_remote, rs._RequestsFromUrlInput)
    assert rs._compute_source_id(0, rs._canonical_source(modified_remote)) != ids[0]


def test_checkpoint_model_camel_case_roundtrip() -> None:
    checkpoint = rs._SourceCheckpoint(
        source_id='sid',
        generation_id='gid',
        url='http://example.com/a.txt',
        raw_offset=42,
        content_offset=42,
        confirmed_keys=['key1'],
        confirmed_requests=[
            rs.Request.from_url(url='http://example.com', method='POST', payload=b'x').model_dump(
                mode='json', by_alias=True
            )
        ],
    )
    dumped = checkpoint.model_dump(mode='json', by_alias=True)
    assert dumped['sourceId'] == 'sid'
    assert dumped['rawOffset'] == 42
    assert dumped['confirmedKeys'] == ['key1']

    restored = rs._SourceCheckpoint.model_validate(dumped)
    request = rs.Request.model_validate(restored.confirmed_requests[0])
    assert request.url == 'http://example.com'
    assert request.method == 'POST'
    assert request.payload == b'x'


class TestLineStreamParser:
    @staticmethod
    def _parse(content: bytes, encoding: str, *, chunk_sizes: list[int] | None = None) -> list[str]:
        source = rs._RequestsFromUrlInput.model_validate({'requestsFromUrl': 'http://example.com/source'})
        parser = rs._LineStreamParser(
            source,
            encoding=encoding,
            wire_start=0,
            content_start=0,
            from_zero=True,
        )
        urls: list[str] = []
        if chunk_sizes is None:
            chunk_sizes = [len(content)]
        cursor = 0
        for size in chunk_sizes:
            for segment in parser.feed(content[cursor : cursor + size]):
                urls.extend(request.url for request in segment.requests)
            cursor += size
        for segment in parser.finish():
            urls.extend(request.url for request in segment.requests)
        return urls

    @pytest.mark.parametrize(
        ('encoded', 'encoding'),
        [
            (BODY, 'identity'),
            (gzip.compress(BODY), 'gzip'),
            (zlib.compress(BODY), 'deflate'),
        ],
        ids=['identity', 'gzip', 'deflate'],
    )
    async def test_encodings_produce_identical_urls(self, encoded: bytes, encoding: str) -> None:
        assert self._parse(encoded, encoding) == LINES

    async def test_byte_sized_chunks_and_multibyte_tail(self) -> None:
        content = 'http://www.x.com/a\ncafé http://www.y.com/b\nhttp://www.z.com/c\n'.encode()
        for chunk_size in range(1, len(content) + 1):
            chunks = [content[i : i + chunk_size] for i in range(0, len(content), chunk_size)]
            assert self._parse(content, 'identity', chunk_sizes=[len(c) for c in chunks]) == [
                'http://www.x.com/a',
                'http://www.y.com/b',
                'http://www.z.com/c',
            ]

        gzipped = gzip.compress(content)
        assert self._parse(gzipped, 'gzip', chunk_sizes=[1] * len(gzipped)) == [
            'http://www.x.com/a',
            'http://www.y.com/b',
            'http://www.z.com/c',
        ]

    async def test_trailing_line_without_newline_flushed_at_finish(self) -> None:
        assert self._parse(b'http://www.a.com\nhttp://www.b.com', 'identity') == [
            'http://www.a.com',
            'http://www.b.com',
        ]

    async def test_jsonl_line_matrix(self) -> None:
        content = (
            b'"http://www.string-line.com"\n'
            b'{"url": "http://www.object-line.com", "method": "POST", "payload": "p", '
            b'"headers": {"h": "v"}, "userData": {"depth": 2, "nested": {"k": "v"}}}\n'
            b'prefix text http://www.regex-line.com suffix\n'
            b'{"noUrl": true}\n'
            b'{broken json\n'
            b'\n'
        )
        source = rs._RequestsFromUrlInput.model_validate(
            {'requestsFromUrl': 'http://example.com/source', 'method': 'PUT', 'userData': {'base': True}}
        )
        parser = rs._LineStreamParser(source, encoding='identity', wire_start=0, content_start=0, from_zero=True)
        collected: dict[str, rs.Request] = {}
        for segment in [*parser.feed(content), *parser.finish()]:
            for request in segment.requests:
                collected[request.url] = request

        assert set(collected) == {
            'http://www.string-line.com',
            'http://www.object-line.com',
            'http://www.regex-line.com',
        }
        assert collected['http://www.string-line.com'].method == 'PUT'
        assert collected['http://www.string-line.com'].user_data['base'] is True
        assert collected['http://www.object-line.com'].method == 'POST'
        assert collected['http://www.object-line.com'].payload == b'p'
        assert collected['http://www.object-line.com'].headers.root == {'h': 'v'}
        assert collected['http://www.object-line.com'].user_data['depth'] == 2

        # Nested userData is deep-copied and isolated between requests.
        nested = collected['http://www.object-line.com'].user_data['nested']
        assert isinstance(nested, dict)
        nested['k'] = 'mutated'
        assert collected['http://www.string-line.com'].user_data['base'] is True

    async def test_invalid_jsonl_object_raises_validation_error(self) -> None:
        source = rs._RequestsFromUrlInput.model_validate({'requestsFromUrl': 'http://example.com/source'})
        with pytest.raises(ValidationError):
            rs._requests_from_line(b'{"url": "http://www.x.com", "method": "WRONG"}', source)

    async def test_line_parsing_matches_full_text_regex(self) -> None:
        content = (
            b'blabla http://www.something.com more\n'
            b'https://www.somethingelse.com\n'
            b'bla http://www.bla.net trailing www.false-positive.com\n'
        )
        assert self._parse(content, 'identity') == [
            'http://www.something.com',
            'https://www.somethingelse.com',
            'http://www.bla.net',
        ]


class TestCheckpointLifecycle:
    async def test_named_open_creates_manifest_and_reuses_generation(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request('/file.txt').respond_with_data(status=200, response_data=BODY)
        source_input = {'requestsFromUrl': httpserver.url_for('/file.txt')}

        first = await ApifyRequestList.open(name='life', request_list_sources_input=[source_input])
        assert await _drain_urls(first) == LINES

        kvs = await KeyValueStore.open()
        store = rs._CheckpointStore('life')
        manifest = rs._CheckpointManifest.model_validate(await kvs.get_value(store.manifest_key()))
        assert manifest.generation_id
        assert manifest.source_ids

        second = await ApifyRequestList.open(name='life', request_list_sources_input=[source_input])
        assert await _drain_urls(second) == LINES

    async def test_changed_source_set_discards_old_generation(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request('/a.txt').respond_with_data(status=200, response_data=BODY)
        httpserver.expect_request('/b.txt').respond_with_data(status=200, response_data=BODY)

        first_input = {'requestsFromUrl': httpserver.url_for('/a.txt')}
        first = await ApifyRequestList.open(name='gen', request_list_sources_input=[first_input])
        assert await _drain_urls(first) == LINES

        kvs = await KeyValueStore.open()
        old_store = rs._CheckpointStore('gen')
        old_manifest = await kvs.get_value(old_store.manifest_key())
        old_source_key = old_store.source_key(0, old_manifest['sourceIds'][0])
        assert await kvs.get_value(old_source_key) is not None

        second_input = {'requestsFromUrl': httpserver.url_for('/b.txt')}
        second = await ApifyRequestList.open(name='gen', request_list_sources_input=[second_input])
        assert await _drain_urls(second) == LINES

        new_manifest = await kvs.get_value(old_store.manifest_key())
        assert new_manifest['generationId'] != old_manifest['generationId']
        assert await kvs.get_value(old_source_key) is None

    async def test_unnamed_open_does_not_touch_storage(self, httpserver: HTTPServer, tmp_path: Any) -> None:
        httpserver.expect_oneshot_request('/file.txt').respond_with_data(status=200, response_data=BODY)
        request_list = await ApifyRequestList.open(
            request_list_sources_input=[{'requestsFromUrl': httpserver.url_for('/file.txt')}]
        )
        assert await _drain_urls(request_list) == LINES
        assert not any(tmp_path.rglob('*'))

    async def test_completed_source_reopened_without_downloads(self, httpserver: HTTPServer) -> None:
        httpserver.expect_oneshot_request('/file.txt').respond_with_data(status=200, response_data=BODY)
        source_input = {
            'requestsFromUrl': httpserver.url_for('/file.txt'),
            'method': 'POST',
            'payload': 'some_payload',
            'headers': {'X-Source': 'yes'},
            'userData': {'depth': 7, 'nested': {'k': 'v'}},
        }

        first = await ApifyRequestList.open(name='done', request_list_sources_input=[source_input])
        first_urls = await _drain_urls(first)

        second = await ApifyRequestList.open(name='done', request_list_sources_input=[source_input])
        second_requests = [r async for r in _request_iter(second)]
        assert [r.url for r in second_requests] == first_urls
        sample = second_requests[10]
        assert sample.method == 'POST'
        assert sample.payload == b'some_payload'
        assert sample.headers.root == {'x-source': 'yes'}
        assert sample.user_data['depth'] == 7
        assert sample.user_data['nested'] == {'k': 'v'}


class TestResume:
    async def test_range_resume_from_seeded_checkpoint(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/big.txt')
        served_bytes = 0

        def counting_range_response(request: Request) -> Response:
            nonlocal served_bytes
            response = _range_response(request, BODY)
            served_bytes += len(response.get_data())
            return response

        httpserver.expect_oneshot_request('/big.txt').respond_with_handler(counting_range_response)
        generation_id, source_key, source_id = await _seed_generation('resume', [{'requestsFromUrl': url}])

        confirmed = 20
        boundary = sum(len(LINES[i]) + 1 for i in range(confirmed))
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            etag='"v1"',
            raw_offset=boundary,
            content_offset=boundary,
            prefix_sha256=hashlib.sha256(BODY[:boundary]).hexdigest(),
            confirmed_keys=LINES[:confirmed],
            confirmed_requests=[
                rs.Request.from_url(url=line).model_dump(mode='json', by_alias=True) for line in LINES[:confirmed]
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(name='resume', request_list_sources_input=[{'requestsFromUrl': url}])
        assert await _drain_urls(request_list) == LINES
        # Only the suffix is transferred; the confirmed prefix costs zero bytes.
        assert 0 < served_bytes < len(BODY)

    async def test_changed_etag_triggers_full_reload(self, httpserver: HTTPServer) -> None:
        new_body = ('\n'.join(f'http://www.changed-{i:03d}.com' for i in range(30)) + '\n').encode()
        url = httpserver.url_for('/changed.txt')
        httpserver.expect_request('/changed.txt').respond_with_data(
            status=200, response_data=new_body, headers={'ETag': '"v2"'}
        )
        generation_id, source_key, source_id = await _seed_generation('changed', [{'requestsFromUrl': url}])
        boundary = 100
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            etag='"v1"',
            raw_offset=boundary,
            content_offset=boundary,
            prefix_sha256=hashlib.sha256(b'old prefix').hexdigest(),
            confirmed_keys=[f'http://www.old-{i}.com' for i in range(5)],
            confirmed_requests=[
                rs.Request.from_url(url=f'http://www.old-{i}.com').model_dump(mode='json', by_alias=True)
                for i in range(5)
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(
            name='changed', request_list_sources_input=[{'requestsFromUrl': url}]
        )
        urls = await _drain_urls(request_list)
        assert urls == [f'http://www.changed-{i:03d}.com' for i in range(30)]
        restored = rs._SourceCheckpoint.model_validate(await kvs.get_value(source_key))
        assert restored.completed is True
        assert restored.etag == '"v2"'

    async def test_replay_on_range_ignoring_server(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/norange.txt')
        httpserver.expect_oneshot_request('/norange.txt').respond_with_data(status=200, response_data=BODY)
        generation_id, source_key, source_id = await _seed_generation('replay', [{'requestsFromUrl': url}])

        confirmed = 25
        boundary = sum(len(LINES[i]) + 1 for i in range(confirmed))
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            raw_offset=boundary,
            content_offset=boundary,
            prefix_sha256=hashlib.sha256(BODY[:boundary]).hexdigest(),
            confirmed_keys=LINES[:confirmed],
            confirmed_requests=[
                rs.Request.from_url(url=line).model_dump(mode='json', by_alias=True) for line in LINES[:confirmed]
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(name='replay', request_list_sources_input=[{'requestsFromUrl': url}])
        assert await _drain_urls(request_list) == LINES

    async def test_tampered_prefix_triggers_reload(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/tampered.txt')
        httpserver.expect_request('/tampered.txt').respond_with_data(status=200, response_data=BODY)
        generation_id, source_key, source_id = await _seed_generation('tampered', [{'requestsFromUrl': url}])

        boundary = sum(len(LINES[i]) + 1 for i in range(15))
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            raw_offset=boundary,
            content_offset=boundary,
            prefix_sha256=hashlib.sha256(b'tampered').hexdigest(),
            confirmed_keys=['http://www.bogus.com'] * 15,
            confirmed_requests=[rs.Request.from_url(url='http://www.bogus.com').model_dump(mode='json', by_alias=True)]
            * 15,
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(
            name='tampered', request_list_sources_input=[{'requestsFromUrl': url}]
        )
        assert await _drain_urls(request_list) == LINES

    async def test_gzip_source_replays_after_interruption(self, httpserver: HTTPServer) -> None:
        decoded = BODY
        gzipped = gzip.compress(decoded)
        url = httpserver.url_for('/gzip.txt')
        httpserver.expect_oneshot_request('/gzip.txt').respond_with_data(
            status=200, response_data=gzipped, headers={'Content-Encoding': 'gzip'}
        )
        generation_id, source_key, source_id = await _seed_generation('gzresume', [{'requestsFromUrl': url}])

        confirmed = 18
        boundary = sum(len(LINES[i]) + 1 for i in range(confirmed))
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            encoding='gzip',
            raw_offset=len(gzipped),
            content_offset=boundary,
            prefix_sha256=hashlib.sha256(decoded[:boundary]).hexdigest(),
            confirmed_keys=LINES[:confirmed],
            confirmed_requests=[
                rs.Request.from_url(url=line).model_dump(mode='json', by_alias=True) for line in LINES[:confirmed]
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(
            name='gzresume', request_list_sources_input=[{'requestsFromUrl': url}]
        )
        assert await _drain_urls(request_list) == LINES

    async def test_shrunk_entity_returning_416_triggers_reload(self, httpserver: HTTPServer) -> None:
        new_lines = [f'http://www.shrunk-{i:03d}.com' for i in range(5)]
        new_body = ('\n'.join(new_lines) + '\n').encode()
        url = httpserver.url_for('/shrunk.txt')

        def shrink_handler(request: Request) -> Response:
            if request.headers.get('Range'):
                return Response(
                    status=416,
                    headers={'Content-Range': f'bytes */{len(new_body)}', 'ETag': '"v2-shrunk"'},
                )
            return Response(new_body, status=200, headers={'ETag': '"v2-shrunk"'})

        httpserver.expect_request('/shrunk.txt').respond_with_handler(shrink_handler)
        generation_id, source_key, source_id = await _seed_generation('shrink', [{'requestsFromUrl': url}])
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            etag='"v1"',
            raw_offset=500,
            content_offset=500,
            prefix_sha256=hashlib.sha256(b'old longer prefix').hexdigest(),
            confirmed_keys=[f'http://www.old-{i}.com' for i in range(5)],
            confirmed_requests=[
                rs.Request.from_url(url=f'http://www.old-{i}.com').model_dump(mode='json', by_alias=True)
                for i in range(5)
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(name='shrink', request_list_sources_input=[{'requestsFromUrl': url}])
        assert await _drain_urls(request_list) == new_lines
        restored = rs._SourceCheckpoint.model_validate(await kvs.get_value(source_key))
        assert restored.completed is True
        assert restored.etag == '"v2-shrunk"'

    async def test_416_at_exact_entity_end_finalizes_identity_source(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/end416.txt')
        request_count = 0

        def handler(_request: Request) -> Response:
            nonlocal request_count
            request_count += 1
            return Response(
                status=416,
                headers={'Content-Range': f'bytes */{len(BODY)}', 'ETag': '"v1"'},
            )

        httpserver.expect_request('/end416.txt').respond_with_handler(handler)
        generation_id, source_key, source_id = await _seed_generation('end416', [{'requestsFromUrl': url}])
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            etag='"v1"',
            raw_offset=len(BODY),
            content_offset=len(BODY),
            prefix_sha256=hashlib.sha256(BODY).hexdigest(),
            confirmed_keys=LINES,
            confirmed_requests=[
                rs.Request.from_url(url=line).model_dump(mode='json', by_alias=True) for line in LINES
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(
            name='end416', request_list_sources_input=[{'requestsFromUrl': url}]
        )
        assert await _drain_urls(request_list) == LINES
        assert request_count == 1
        restored = rs._SourceCheckpoint.model_validate(await kvs.get_value(source_key))
        assert restored.completed is True

    async def test_416_with_total_beyond_boundary_is_not_finalized(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/long416.txt')
        request_count = 0

        def handler(request: Request) -> Response:
            nonlocal request_count
            request_count += 1
            if request.headers.get('Range'):
                # Non-compliant: claims a longer entity while refusing the range.
                return Response(
                    status=416,
                    headers={'Content-Range': f'bytes */{len(BODY) + 50}', 'ETag': '"v1"'},
                )
            return Response(BODY, status=200, headers={'ETag': '"v1"'})

        httpserver.expect_request('/long416.txt').respond_with_handler(handler)
        generation_id, source_key, source_id = await _seed_generation('long416', [{'requestsFromUrl': url}])
        boundary = sum(len(LINES[i]) + 1 for i in range(20))
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            etag='"v1"',
            raw_offset=boundary,
            content_offset=boundary,
            prefix_sha256=hashlib.sha256(BODY[:boundary]).hexdigest(),
            confirmed_keys=LINES[:20],
            confirmed_requests=[
                rs.Request.from_url(url=line).model_dump(mode='json', by_alias=True) for line in LINES[:20]
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(
            name='long416', request_list_sources_input=[{'requestsFromUrl': url}]
        )
        assert await _drain_urls(request_list) == LINES
        # 416 with a longer total triggers a fresh full GET rather than a finalize.
        assert request_count == 2

    async def test_416_for_compressed_checkpoint_replays_full_entity(self, httpserver: HTTPServer) -> None:
        decoded = BODY
        gzipped = gzip.compress(decoded)
        url = httpserver.url_for('/gz416.txt')
        request_count = 0

        def handler(request: Request) -> Response:
            nonlocal request_count
            request_count += 1
            if request.headers.get('Range'):
                return Response(status=416, headers={'Content-Range': f'bytes */{len(gzipped)}'})
            return Response(gzipped, status=200, headers={'Content-Encoding': 'gzip'})

        httpserver.expect_request('/gz416.txt').respond_with_handler(handler)
        generation_id, source_key, source_id = await _seed_generation('gz416', [{'requestsFromUrl': url}])

        confirmed = 18
        boundary = sum(len(LINES[i]) + 1 for i in range(confirmed))
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            encoding='gzip',
            raw_offset=len(gzipped),
            content_offset=boundary,
            prefix_sha256=hashlib.sha256(decoded[:boundary]).hexdigest(),
            confirmed_keys=LINES[:confirmed],
            confirmed_requests=[
                rs.Request.from_url(url=line).model_dump(mode='json', by_alias=True) for line in LINES[:confirmed]
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(name='gz416', request_list_sources_input=[{'requestsFromUrl': url}])
        assert await _drain_urls(request_list) == LINES
        assert request_count == 2

    async def test_wire_spans_replay_verifies_with_single_full_get(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/spans.txt')
        request_count = 0

        def norange_handler(_request: Request) -> Response:
            nonlocal request_count
            request_count += 1
            return Response(BODY, status=200)

        httpserver.expect_request('/spans.txt').respond_with_handler(norange_handler)
        generation_id, source_key, source_id = await _seed_generation('spans', [{'requestsFromUrl': url}])

        boundary_10 = sum(len(LINES[i]) + 1 for i in range(10))
        boundary_20 = sum(len(LINES[i]) + 1 for i in range(20))
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            raw_offset=boundary_20,
            content_offset=boundary_20,
            prefix_sha256=hashlib.sha256(BODY[:boundary_10]).hexdigest(),
            wire_spans=[
                rs._WireSpan(
                    start=boundary_10,
                    end=boundary_20,
                    digest=hashlib.sha256(BODY[boundary_10:boundary_20]).hexdigest(),
                )
            ],
            confirmed_keys=LINES[:20],
            confirmed_requests=[
                rs.Request.from_url(url=line).model_dump(mode='json', by_alias=True) for line in LINES[:20]
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(name='spans', request_list_sources_input=[{'requestsFromUrl': url}])
        assert await _drain_urls(request_list) == LINES
        # Content unchanged: one Range probe answered with 200 must suffice, no reload.
        assert request_count == 1
        restored = rs._SourceCheckpoint.model_validate(await kvs.get_value(source_key))
        assert restored.completed is True

    async def test_jsonl_source_replays_from_confirmed_prefix(self, httpserver: HTTPServer) -> None:
        lines = [
            '"http://www.jl-0.com"',
            '{"url": "http://www.jl-1.com", "method": "POST", "userData": {"depth": 1}}',
            '"http://www.jl-2.com"',
            '{"url": "http://www.jl-3.com"}',
        ]
        body = ('\n'.join(lines) + '\n').encode()
        url = httpserver.url_for('/jl.txt')
        httpserver.expect_oneshot_request('/jl.txt').respond_with_data(status=200, response_data=body)

        source = rs._RequestsFromUrlInput.model_validate({'requestsFromUrl': url})
        expected: list[rs.Request] = []
        line_bytes = body.splitlines(keepends=True)
        for raw_line in line_bytes:
            expected.extend(rs._requests_from_line(raw_line.rstrip(b'\n'), source))
        boundary = len(line_bytes[0]) + len(line_bytes[1])
        prefix_keys = [request.unique_key for request in expected[:2]]
        prefix_requests = [request.model_dump(mode='json', by_alias=True) for request in expected[:2]]

        generation_id, source_key, source_id = await _seed_generation('jl', [{'requestsFromUrl': url}])
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            raw_offset=boundary,
            content_offset=boundary,
            prefix_sha256=hashlib.sha256(body[:boundary]).hexdigest(),
            confirmed_keys=prefix_keys,
            confirmed_requests=prefix_requests,
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        request_list = await ApifyRequestList.open(name='jl', request_list_sources_input=[{'requestsFromUrl': url}])
        collected = await _drain_urls(request_list)
        assert collected == [request.url for request in expected]
        post_object = collected.index('http://www.jl-1.com')
        assert post_object == 1


class TestStatusRetry:
    async def test_transient_5xx_is_retried(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/flaky500.txt')
        attempts = 0

        def flaky_status(request: Request) -> Response:
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                return Response(status=503)
            return _range_response(request, BODY)

        httpserver.expect_request('/flaky500.txt').respond_with_handler(flaky_status)
        request_list = await ApifyRequestList.open(
            name='http503', request_list_sources_input=[{'requestsFromUrl': url}]
        )
        assert await _drain_urls(request_list) == LINES
        assert attempts == 3

    async def test_persistent_5xx_fails_and_keeps_checkpoint(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/always500.txt')
        httpserver.expect_request('/always500.txt').respond_with_response(Response(status=500))

        generation_id, source_key, source_id = await _seed_generation('http500', [{'requestsFromUrl': url}])
        boundary = sum(len(LINES[i]) + 1 for i in range(5))
        checkpoint = rs._SourceCheckpoint(
            source_id=source_id,
            generation_id=generation_id,
            url=url,
            raw_offset=boundary,
            content_offset=boundary,
            prefix_sha256=hashlib.sha256(BODY[:boundary]).hexdigest(),
            confirmed_keys=LINES[:5],
            confirmed_requests=[
                rs.Request.from_url(url=line).model_dump(mode='json', by_alias=True) for line in LINES[:5]
            ],
        )
        kvs = await KeyValueStore.open()
        await kvs.set_value(source_key, checkpoint.model_dump(mode='json', by_alias=True), 'application/json')

        with pytest.raises((rs._RetryableDownloadError, BaseExceptionGroup)) as exc_info:
            await ApifyRequestList.open(name='http500', request_list_sources_input=[{'requestsFromUrl': url}])

        def contains_retryable(exc: BaseException) -> bool:
            if isinstance(exc, rs._RetryableDownloadError):
                return True
            return isinstance(exc, BaseExceptionGroup) and any(contains_retryable(sub) for sub in exc.exceptions)

        assert contains_retryable(exc_info.value)
        rs._SourceCheckpoint.model_validate(await kvs.get_value(source_key))

    async def test_response_changed_ignores_compressed_content_length(self) -> None:
        class _Headers:
            def __init__(self, headers: dict[str, str | None]) -> None:
                self._headers = {key.lower(): value for key, value in headers.items()}

            def get(self, key: str) -> str | None:
                return self._headers.get(key.lower())

        class _Response:
            def __init__(self, headers: _Headers) -> None:
                self.headers = headers

        checkpoint = rs._SourceCheckpoint(
            source_id='sid',
            generation_id='gid',
            url='http://example.com/a',
            raw_offset=1000,
            content_offset=1000,
        )

        compressed = _Response(_Headers({'Content-Encoding': 'gzip', 'Content-Length': '200'}))
        assert (
            rs._SourceRunner._response_changed(compressed, checkpoint) is False  # ty: ignore[invalid-argument-type]
        )
        identity = _Response(_Headers({'Content-Encoding': None, 'Content-Length': '200'}))
        assert rs._SourceRunner._response_changed(identity, checkpoint) is True  # ty: ignore[invalid-argument-type]


class _FlakyFirstAttemptClient(ImpitHttpClient):
    """Breaks the first stream after `fail_after_bytes`, then behaves normally."""

    def __init__(self, *, host: str, fail_after_bytes: int, exc: type[BaseException] = ConnectionResetError) -> None:
        super().__init__()
        self._host = host
        self._fail_after_bytes = fail_after_bytes
        self._exc = exc
        self._first = True
        self.requests: list[dict[str, str]] = []

    def stream(  # ty: ignore[invalid-method-override]
        self,
        url: str,
        *,
        method: HttpMethod = 'GET',
        headers: dict[str, str] | None = None,
        **kwargs: Any,
    ) -> Any:
        self.requests.append({'url': url, 'range': (headers or {}).get('Range', '')})
        context = super().stream(url, method=method, headers=headers, **kwargs)
        if not (self._first and url.startswith(self._host)):
            return context
        self._first = False
        limit = self._fail_after_bytes
        exc = self._exc

        class _Broken:
            async def __aenter__(self) -> Any:
                response = await context.__aenter__()
                original = response.read_stream

                async def broken_iter() -> Any:
                    count = 0
                    async for chunk in original():
                        remaining = limit - count
                        if len(chunk) <= remaining:
                            yield chunk
                            count += len(chunk)
                        else:
                            if remaining > 0:
                                yield chunk[:remaining]
                            raise exc('simulated interruption')

                response.read_stream = broken_iter  # ty: ignore[invalid-assignment]
                return response

            async def __aexit__(self, *args: Any) -> Any:
                return await context.__aexit__(*args)

        return _Broken()


class TestRetriesAndFailures:
    async def test_interrupted_download_resumes_via_range(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/big.txt')
        httpserver.expect_request('/big.txt').respond_with_handler(lambda request: _range_response(request, BODY))
        client = _FlakyFirstAttemptClient(host=url, fail_after_bytes=300)

        request_list = await ApifyRequestList.open(
            name='flaky',
            request_list_sources_input=[{'requestsFromUrl': url}],
            http_client=client,
        )
        assert await _drain_urls(request_list) == LINES
        assert len(client.requests) == 2
        assert client.requests[0]['range'] == ''
        assert client.requests[1]['range'].startswith('bytes=')

    async def test_checkpoint_save_failure_keeps_old_record(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/big.txt')
        httpserver.expect_request('/big.txt').respond_with_handler(lambda request: _range_response(request, BODY))

        first = await ApifyRequestList.open(name='savefail', request_list_sources_input=[{'requestsFromUrl': url}])
        assert await _drain_urls(first) == LINES

        kvs = await KeyValueStore.open()
        store = rs._CheckpointStore('savefail')
        manifest = rs._CheckpointManifest.model_validate(await kvs.get_value(store.manifest_key()))
        source_key = store.source_key(0, manifest.source_ids[0])
        good_record = await kvs.get_value(source_key)
        assert good_record['completed'] is True

        # Truncate the checkpoint to an earlier boundary so the next open has work to persist.
        boundary = sum(len(LINES[i]) + 1 for i in range(10))
        truncated = dict(good_record)
        truncated['completed'] = False
        truncated['confirmedKeys'] = truncated['confirmedKeys'][:10]
        truncated['confirmedRequests'] = truncated['confirmedRequests'][:10]
        truncated['rawOffset'] = boundary
        truncated['contentOffset'] = boundary
        truncated['prefixSha256'] = hashlib.sha256(BODY[:boundary]).hexdigest()
        truncated['wireSpans'] = []
        await kvs.set_value(source_key, truncated)

        failures = 0
        original_set_value = kvs.set_value
        original_get_value = kvs.get_value

        async def failing_set_value(key: str, value: Any, *args: Any, **kwargs: Any) -> None:
            nonlocal failures
            if key == source_key:
                failures += 1
                raise RuntimeError('simulated KVS failure')
            await original_set_value(key, value, *args, **kwargs)

        kvs.set_value = failing_set_value  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]
        try:
            request_list = await ApifyRequestList.open(
                name='savefail', request_list_sources_input=[{'requestsFromUrl': url}]
            )
            assert await _drain_urls(request_list) == LINES
        finally:
            kvs.set_value = original_set_value  # type: ignore[method-assign]  # ty: ignore[invalid-assignment]

        assert failures >= 1
        # The old truncated record remains intact and parseable (no half-write).
        rs._SourceCheckpoint.model_validate(await original_get_value(source_key))

    async def test_cancellation_resumes_same_generation(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/big.txt')

        def slow_handler(_request: Request) -> Response:
            import time

            def generate() -> Any:
                for i in range(0, len(BODY), 200):
                    time.sleep(0.05)
                    yield BODY[i : i + 200]

            return Response(generate(), status=200, headers={'ETag': '"v1"', 'Accept-Ranges': 'bytes'})

        httpserver.expect_request('/big.txt').respond_with_handler(slow_handler)

        open_task = asyncio.create_task(
            ApifyRequestList.open(
                name='cancelled',
                request_list_sources_input=[{'requestsFromUrl': url}],
            )
        )
        await asyncio.sleep(0.1)
        kvs = await KeyValueStore.open()
        store = rs._CheckpointStore('cancelled')
        manifest_before = await kvs.get_value(store.manifest_key())
        assert manifest_before is not None
        generation_before = manifest_before['generationId']

        open_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await open_task

        manifest_after = await kvs.get_value(store.manifest_key())
        assert manifest_after is not None
        # Cancellation and reopen keep the same source-set generation.
        assert manifest_after['generationId'] == generation_before

        httpserver.clear()
        httpserver.expect_request('/big.txt').respond_with_handler(lambda request: _range_response(request, BODY))
        request_list = await ApifyRequestList.open(
            name='cancelled', request_list_sources_input=[{'requestsFromUrl': url}]
        )
        assert await _drain_urls(request_list) == LINES


class TestMerging:
    async def test_deterministic_order_regardless_of_completion(self, httpserver: HTTPServer) -> None:
        slow_urls = [f'http://www.slow-{i}.com' for i in range(5)]
        fast_urls = [f'http://www.fast-{i}.com' for i in range(5)]

        def slow_handler(_request: Request) -> Response:
            import time

            time.sleep(0.2)
            return Response(('\n'.join(slow_urls) + '\n').encode(), status=200)

        httpserver.expect_request('/slow.txt').respond_with_handler(slow_handler)
        httpserver.expect_request('/fast.txt').respond_with_data(
            status=200, response_data=('\n'.join(fast_urls) + '\n').encode()
        )

        sources = [
            {'url': 'http://www.inline.com'},
            {'requestsFromUrl': httpserver.url_for('/slow.txt')},
            {'requestsFromUrl': httpserver.url_for('/fast.txt')},
            {'url': 'http://www.fast-2.com'},
        ]
        for _ in range(3):
            request_list = await ApifyRequestList.open(name='merge', request_list_sources_input=sources)
            assert await _drain_urls(request_list) == [
                'http://www.inline.com',
                'http://www.fast-2.com',
                *slow_urls,
                'http://www.fast-0.com',
                'http://www.fast-1.com',
                'http://www.fast-3.com',
                'http://www.fast-4.com',
            ]

    async def test_global_unique_key_dedup_first_wins(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request('/a.txt').respond_with_data(
            status=200, response_data=b'http://www.dup.com\nhttp://www.a-only.com\n'
        )
        httpserver.expect_request('/b.txt').respond_with_data(
            status=200, response_data=b'http://www.dup.com\nhttp://www.b-only.com\n'
        )
        sources = [
            {'url': 'http://www.dup.com'},
            {'requestsFromUrl': httpserver.url_for('/a.txt')},
            {'requestsFromUrl': httpserver.url_for('/b.txt')},
        ]
        request_list = await ApifyRequestList.open(name='dedup', request_list_sources_input=sources)
        assert await _drain_urls(request_list) == [
            'http://www.dup.com',
            'http://www.a-only.com',
            'http://www.b-only.com',
        ]


class TestPersistState:
    async def test_persist_state_flushes_mid_load_and_unregisters(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/big.txt')

        def streaming_handler(_request: Request) -> Response:
            def generate() -> Any:
                for i in range(0, len(BODY), 200):
                    import time

                    time.sleep(0.02)
                    yield BODY[i : i + 200]

            return Response(generate(), status=200, headers={'ETag': '"v1"', 'Accept-Ranges': 'bytes'})

        httpserver.expect_request('/big.txt').respond_with_handler(streaming_handler)

        open_task = asyncio.create_task(
            ApifyRequestList.open(
                name='persist-event',
                request_list_sources_input=[{'requestsFromUrl': url}],
            )
        )
        await asyncio.sleep(0.05)
        event_manager = service_locator.get_event_manager()
        kvs = await KeyValueStore.open()
        store = rs._CheckpointStore('persist-event')

        # Invoke the registered listeners directly (the LocalEventManager only emits within its context).
        wrappers = event_manager._listeners_to_wrappers[Event.PERSIST_STATE]
        assert wrappers
        for wrapper_group in wrappers.values():
            for wrapper in wrapper_group:
                await wrapper(EventPersistStateData(is_migrating=True))

        # Observe the forced mid-load boundary before the load completes.
        manifest = await kvs.get_value(store.manifest_key())
        source_key = store.source_key(0, manifest['sourceIds'][0])
        mid_record = None
        for _ in range(400):
            record = await kvs.get_value(source_key)
            if record is not None and record.get('rawOffset') and not record.get('completed'):
                mid_record = record
                break
            await asyncio.sleep(0.005)

        request_list = await open_task

        # Our loader listener is unregistered as soon as open() returns (before the
        # RequestList itself registers its own progress listener on the first fetch).
        listeners = event_manager._listeners_to_wrappers.get(Event.PERSIST_STATE, {})
        assert not listeners

        kvs = await KeyValueStore.open()
        store = rs._CheckpointStore('persist-event')
        # The forced mid-load flush captured a real mid-source boundary, not just the end.
        assert mid_record is not None
        assert 0 < mid_record['rawOffset'] < len(BODY)
        assert mid_record['completed'] is False
        assert len(mid_record['confirmedKeys']) > 0
        record = await kvs.get_value(source_key)
        assert record is not None
        checkpoint = rs._SourceCheckpoint.model_validate(record)
        assert checkpoint.completed is True
        assert await _drain_urls(request_list) == LINES

    async def test_custom_client_used_for_range_requests(self, httpserver: HTTPServer) -> None:
        url = httpserver.url_for('/big.txt')
        httpserver.expect_request('/big.txt').respond_with_handler(lambda request: _range_response(request, BODY))
        client = _FlakyFirstAttemptClient(host=url, fail_after_bytes=250)

        await ApifyRequestList.open(
            name='client',
            request_list_sources_input=[{'requestsFromUrl': url, 'headers': {'X-Source': 's'}, 'userData': {'k': 'v'}}],
            http_client=client,
        )
        assert len(client.requests) == 2
        assert client.requests[1]['range'].startswith('bytes=')


async def _request_iter(request_list: ApifyRequestList) -> Any:
    while request := await request_list.fetch_next_request():
        yield request
