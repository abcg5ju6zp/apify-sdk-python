from __future__ import annotations

from urllib.parse import ParseResult, urlparse

import pytest
from scrapy import Request, Spider
from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured

from apify import ProxyConfiguration, ProxyInfo
from apify.scrapy.middlewares import ApifyHttpProxyMiddleware


class DummySpider(Spider):
    name = 'dummy_spider'


@pytest.fixture
def middleware() -> ApifyHttpProxyMiddleware:
    """Fixture to create an Apify HTTP proxy middleware."""
    proxy_settings = {'useApifyProxy': True}
    return ApifyHttpProxyMiddleware(proxy_settings)


@pytest.fixture
def crawler(monkeypatch: pytest.MonkeyPatch) -> Crawler:
    """Fixture to create a Scrapy crawler."""
    crawler = Crawler(DummySpider)
    monkeypatch.setattr(crawler, 'settings', {})
    return crawler


@pytest.fixture
def dummy_request() -> Request:
    """Fixture to create a "dummy" Scrapy spider."""
    return Request('https://example.com')


@pytest.fixture
def proxy_configuration() -> ProxyConfiguration:
    """Fixture to create an Apify ProxyConfiguration object."""
    return ProxyConfiguration()


@pytest.mark.parametrize(
    ('settings', 'expected_exception'),
    [
        ({'APIFY_PROXY_SETTINGS': {'useApifyProxy': True}}, None),
        ({'APIFY_PROXY_SETTINGS': {'useApifyProxy': True, 'apifyProxyGroups': []}}, None),
        ({}, NotConfigured),
        ({'a': 1}, NotConfigured),
        ({'APIFY_PROXY_SETTINGS': {}}, NotConfigured),
        ({'APIFY_PROXY_SETTINGS': {'useApifyProxy': None}}, NotConfigured),
        ({'APIFY_PROXY_SETTINGS': {'useApifyProxy': False}}, NotConfigured),
    ],
    ids=[
        'valid_proxy_settings_enabled',
        'valid_proxy_settings_with_groups',
        'no_settings',
        'irrelevant_setting',
        'empty_proxy_settings',
        'use_apify_proxy_none',
        'use_apify_proxy_false',
    ],
)
def test_initialization_from_crawler(
    crawler: Crawler,
    monkeypatch: pytest.MonkeyPatch,
    settings: dict,
    expected_exception: type[Exception] | None,
) -> None:
    monkeypatch.setattr(crawler, 'settings', settings)

    if expected_exception is None:
        middleware = ApifyHttpProxyMiddleware.from_crawler(crawler)
        assert middleware._proxy_settings == settings['APIFY_PROXY_SETTINGS']

    else:
        with pytest.raises(expected_exception):
            ApifyHttpProxyMiddleware.from_crawler(crawler)


@pytest.mark.parametrize(
    'expected_proxy_url',
    ['http://username:password@proxy.example.com:8080', 'http://hsdfgds:52354325@proxy.apify.com:5748'],
    ids=['example_proxy', 'apify_proxy'],
)
async def test_retrieves_new_proxy_url(
    monkeypatch: pytest.MonkeyPatch,
    middleware: ApifyHttpProxyMiddleware,
    proxy_configuration: ProxyConfiguration,
    expected_proxy_url: str,
) -> None:
    async def mock_new_url() -> str:
        return expected_proxy_url

    monkeypatch.setattr(proxy_configuration, 'new_url', mock_new_url)
    middleware._proxy_cfg_internal = proxy_configuration
    proxy_url = await middleware._get_new_proxy_url()
    assert proxy_url == urlparse(expected_proxy_url)


@pytest.mark.parametrize(
    ('proxy_url', 'expected_exception', 'expected_request_header'),
    [
        ('http://username:password@proxy.example.com:8080', None, b'Basic dXNlcm5hbWU6cGFzc3dvcmQ='),
        ('http://user123:pass456@proxy.apify.com:5748', None, b'Basic dXNlcjEyMzpwYXNzNDU2'),
        ('http://@proxy.example.com:2943', ValueError, b''),
    ],
    ids=['valid_example_proxy', 'valid_apify_proxy', 'invalid_proxy_missing_credentials'],
)
async def test_process_request_with_proxy(
    *,
    monkeypatch: pytest.MonkeyPatch,
    middleware: ApifyHttpProxyMiddleware,
    dummy_request: Request,
    proxy_url: str,
    expected_exception: type[Exception] | None,
    expected_request_header: bytes,
) -> None:
    async def mock_get_proxy_for_request() -> tuple[ParseResult, ProxyInfo | None]:
        return urlparse(proxy_url), None

    monkeypatch.setattr(middleware, '_get_proxy_for_request', mock_get_proxy_for_request)

    if expected_exception is None:
        await middleware.process_request(dummy_request)
        assert dummy_request.meta['proxy'] == proxy_url
        assert dummy_request.headers[b'Proxy-Authorization'] == expected_request_header
    else:
        with pytest.raises(expected_exception):
            await middleware.process_request(dummy_request)


@pytest.mark.parametrize(
    'exception',
    [TunnelError(), ValueError()],
    ids=['tunnel_error', 'value_error'],
)
def test_handles_exceptions(
    middleware: ApifyHttpProxyMiddleware,
    dummy_request: Request,
    exception: Exception,
) -> None:
    returned_value = middleware.process_exception(dummy_request, exception)
    assert returned_value is None


async def test_sticky_session_goes_through_confirmed_lease(
    monkeypatch: pytest.MonkeyPatch,
    proxy_configuration: ProxyConfiguration,
) -> None:
    middleware = ApifyHttpProxyMiddleware({'useApifyProxy': True, 'apifyProxySession': 'sess1'})
    proxy_info = ProxyInfo(
        url='http://session-sess1:password@proxy.apify.com:8000',
        scheme='http',
        hostname='proxy.apify.com',
        port=8000,
        username='session-sess1',
        password='password',
        session_id='sess1',
        logical_session_id='sess1',
        lease_generation=1,
    )

    async def mock_new_proxy_info(
        session_id: str | None = None,
        _request: object = None,
        _proxy_tier: int | None = None,
    ) -> ProxyInfo:
        assert session_id == 'sess1'
        return proxy_info

    monkeypatch.setattr(proxy_configuration, 'new_proxy_info', mock_new_proxy_info)
    middleware._proxy_cfg_internal = proxy_configuration

    url, info = await middleware._get_proxy_for_request()

    assert info is proxy_info
    assert url.username == 'session-sess1'


async def test_session_rotation_is_reported_on_request_meta(
    monkeypatch: pytest.MonkeyPatch,
    middleware: ApifyHttpProxyMiddleware,
    dummy_request: Request,
) -> None:
    proxy_url = 'http://session-sess1~g2:password@proxy.apify.com:8000'
    proxy_info = ProxyInfo(
        url=proxy_url,
        scheme='http',
        hostname='proxy.apify.com',
        port=8000,
        username='session-sess1~g2',
        password='password',
        session_id='sess1~g2',
        logical_session_id='sess1',
        lease_generation=2,
        session_rotated=True,
    )

    async def mock_get_proxy_for_request() -> tuple[ParseResult, ProxyInfo | None]:
        return urlparse(proxy_url), proxy_info

    monkeypatch.setattr(middleware, '_get_proxy_for_request', mock_get_proxy_for_request)

    await middleware.process_request(dummy_request)

    assert dummy_request.meta['proxy'] == proxy_url
    assert dummy_request.meta['apify_proxy_session_rotated'] is True
    assert dummy_request.meta['apify_proxy_session_generation'] == 2


async def test_no_session_keeps_sessionless_rotation(
    monkeypatch: pytest.MonkeyPatch,
    middleware: ApifyHttpProxyMiddleware,
    proxy_configuration: ProxyConfiguration,
) -> None:
    expected_url = 'http://auto:password@proxy.apify.com:8000'

    async def mock_new_url() -> str:
        return expected_url

    monkeypatch.setattr(proxy_configuration, 'new_url', mock_new_url)
    middleware._proxy_cfg_internal = proxy_configuration

    url, proxy_info = await middleware._get_proxy_for_request()

    assert proxy_info is None
    assert url.geturl() == expected_url
