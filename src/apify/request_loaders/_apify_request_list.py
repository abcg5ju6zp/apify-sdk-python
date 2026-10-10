from __future__ import annotations

from typing import Any

from crawlee.http_clients import HttpClient, ImpitHttpClient
from crawlee.request_loaders import RequestList

from apify import Request
from apify._utils import docs_group

from ._remote_sources import (
    URL_NO_COMMAS_REGEX,
    _RequestsFromUrlInput,
    _SimpleUrlInput,
    append_remote_requests,
    url_input_adapter,
)

__all__ = ['URL_NO_COMMAS_REGEX', 'ApifyRequestList', 'url_input_adapter']


@docs_group('Request loaders')
class ApifyRequestList(RequestList):
    """A request list that can be constructed from the standard Apify `requestListSources` Actor input format.

    This extends the Crawlee `RequestList` with the ability to parse the request list sources input commonly
    used in Apify Actors. It supports two kinds of entries:

    - **Direct URLs** - entries with a `url` key are converted to requests directly.
    - **Remote URL lists** - entries with a `requestsFromUrl` key point to a remote resource (a plain-text
      file with URLs, or a JSONL file where each line is a URL string or a JSON request object containing a
      `url` field and optional `method`, `payload`, `headers`, and `userData`). The resource is streamed and
      all URLs found in the response body are extracted and converted to requests.

    Both kinds of entries can optionally specify `method`, `payload`, `headers`, and `userData` fields that will be
    applied to every request created from that entry.

    Requests are deduplicated globally by their `unique_key`: in the deterministic merge order (direct URL
    entries first, then remote sources in their input order and the request order inside each source), the
    first occurrence of each identity wins regardless of the concurrent download completion order.

    When a `name` is provided, the remote sources are loaded resumably: each source keeps a checkpoint with
    its stable identity, response validation metadata (`ETag` / `Last-Modified`) and the confirmed parsing
    boundary in the default key-value store. Checkpoints are refreshed on the `PERSIST_STATE` event, so after
    a migration or a reboot the sources resume from the confirmed boundary - using range requests when the
    server supports them, replaying and verifying the prefix from the start otherwise, and fully reloading
    sources whose content changed. Without a `name`, the loading is in-memory only.

    ### Usage

    ```python
    from apify import Actor
    from apify.request_loaders import ApifyRequestList

    async with Actor:
        actor_input = await Actor.get_input() or {}
        request_list = await ApifyRequestList.open(
            request_list_sources_input=actor_input.get('requestListSources', []),
        )
    ```
    """

    @classmethod
    async def open(
        cls,
        *,
        name: str | None = None,
        request_list_sources_input: list[dict[str, Any]] | None = None,
        http_client: HttpClient | None = None,
    ) -> ApifyRequestList:
        """Create a new `ApifyRequestList` from the standard Apify request list sources input.

        Each entry in `request_list_sources_input` is a dict with either a `url` key (for a direct URL) or
        a `requestsFromUrl` key (for a remote resource whose response body is scanned for URLs, or which
        contains JSONL request records). Optional keys `method`, `payload`, `headers`, and `userData` are
        applied to every request produced from that entry.

        Args:
            name: An optional name for the request list. When provided, remote source checkpoints are
                persisted in the default key-value store under a derived key and reused across migrations,
                reboots and repeated calls with the same input.
            request_list_sources_input: A list of request source dicts in the standard Apify format. Each dict must
                contain either a `url` key or a `requestsFromUrl` key. If `None` or empty, an empty request list
                is returned.
            http_client: HTTP client used to fetch remote URL lists (entries with `requestsFromUrl`). Defaults to
                `ImpitHttpClient` if not provided. The same client (including its proxy and authentication
                configuration) is used for resumable range requests.

        Returns:
            A new `ApifyRequestList` populated with the resolved, deduplicated requests.
        """
        request_list_sources_input = request_list_sources_input or []

        if not http_client:
            http_client = ImpitHttpClient()

        url_inputs = url_input_adapter.validate_python(request_list_sources_input)

        simple_url_inputs = [url_input for url_input in url_inputs if isinstance(url_input, _SimpleUrlInput)]
        remote_url_inputs = [url_input for url_input in url_inputs if isinstance(url_input, _RequestsFromUrlInput)]

        requests: list[Request] = []
        seen_unique_keys: set[str] = set()
        for request_input in simple_url_inputs:
            request = cls._create_request_from_input(request_input)
            if request.unique_key not in seen_unique_keys:
                seen_unique_keys.add(request.unique_key)
                requests.append(request)

        await append_remote_requests(
            requests,
            seen_unique_keys,
            name=name,
            remote_inputs=remote_url_inputs,
            http_client=http_client,
        )

        return ApifyRequestList(name=name, requests=requests)

    @staticmethod
    def _create_request_from_input(request_input: _SimpleUrlInput) -> Request:
        """Create a `Request` object from a simple URL input entry."""
        return Request.from_url(
            method=request_input.method,
            url=request_input.url,
            payload=request_input.payload.encode('utf-8'),
            headers=request_input.headers,
            user_data=request_input.user_data,
        )
