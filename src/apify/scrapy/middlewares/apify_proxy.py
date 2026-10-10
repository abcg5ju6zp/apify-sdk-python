from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import ParseResult, urlparse

from scrapy.core.downloader.handlers.http11 import TunnelError
from scrapy.exceptions import NotConfigured

from apify import Actor, ProxyConfiguration, ProxyInfo
from apify.scrapy.utils import get_basic_auth_header

if TYPE_CHECKING:
    from scrapy import Request
    from scrapy.crawler import Crawler


class ApifyHttpProxyMiddleware:
    """Apify HTTP proxy middleware for Scrapy.

    This middleware enhances request processing by adding a 'proxy' field to the request's meta and an authentication
    header. It draws inspiration from the `HttpProxyMiddleware` included by default in Scrapy projects. The proxy URL
    is sourced from the settings under the `APIFY_PROXY_SETTINGS` key. The value of this key, a dictionary, should be
    provided by the Actor input. An example of the proxy settings:

    proxy_settings = {'useApifyProxy': true, 'apifyProxyGroups': []}

    When `apifyProxySession` is provided in the proxy settings, all requests share the same sticky proxy session,
    backed by the proxy configuration's persisted session lease. If the leased exit dies or switches, the lease is
    rotated and the rotation is reported on the request meta (`apify_proxy_session_rotated=True`).
    """

    def __init__(self, proxy_settings: dict) -> None:
        """Create a new instance.

        Args:
            proxy_settings: Dictionary containing proxy settings, provided by the Actor input.
        """
        self._proxy_settings = proxy_settings
        self._proxy_cfg_internal: ProxyConfiguration | None = None

    @classmethod
    def from_crawler(cls: type[ApifyHttpProxyMiddleware], crawler: Crawler) -> ApifyHttpProxyMiddleware:
        """Create an instance of ApifyHttpProxyMiddleware from a Scrapy Crawler.

        Args:
            crawler: Scrapy Crawler object.

        Returns: Instance of the class.
        """
        proxy_settings: dict | None = crawler.settings.get('APIFY_PROXY_SETTINGS')

        if proxy_settings is None:
            Actor.log.info(
                'ApifyHttpProxyMiddleware is not going to be used. Object "proxyConfiguration" is probably missing '
                'in the Actor input.'
            )
            raise NotConfigured

        use_apify_proxy = proxy_settings.get('useApifyProxy', False)

        if use_apify_proxy is not True:
            Actor.log.info(
                'ApifyHttpProxyMiddleware is not going to be used. Actor input field '
                '"proxyConfiguration.useApifyProxy" is set to False.'
            )
            raise NotConfigured

        return cls(proxy_settings)

    async def process_request(self, request: Request) -> None:
        """Process a Scrapy request by assigning a new proxy.

        Args:
            request: Scrapy Request object.

        Raises:
            ValueError: If username and password are not provided in the proxy URL.
        """
        url, proxy_info = await self._get_proxy_for_request()

        if not (url.username and url.password):
            raise ValueError('Username and password must be provided in the proxy URL.')

        request.meta['proxy'] = url.geturl()
        basic_auth_header = get_basic_auth_header(url.username, url.password)
        request.headers[b'Proxy-Authorization'] = basic_auth_header

        if proxy_info is not None and proxy_info.session_rotated:
            Actor.log.warning(
                'ApifyHttpProxyMiddleware: sticky proxy session %r was rotated to generation %s (physical session '
                '%r) because the previous exit was unusable; site-side session state should be treated as new.',
                proxy_info.logical_session_id,
                proxy_info.lease_generation,
                proxy_info.session_id,
            )
            request.meta['apify_proxy_session_rotated'] = True
            request.meta['apify_proxy_session_generation'] = proxy_info.lease_generation

    def process_exception(
        self,
        request: Request,
        exception: Exception,
    ) -> None:
        """Process an exception that occurs during request processing.

        Args:
            request: Scrapy Request object.
            exception: Exception object.

        Returns:
            Returning None, meaning Scrapy will continue processing this exception, executing any other
            process_exception() methods of installed middleware, until no middleware is left and the default
            exception handling kicks in.
        """
        if isinstance(exception, TunnelError):
            Actor.log.warning(
                f'ApifyHttpProxyMiddleware: TunnelError occurred for request="{request}", '
                f'reason="{exception}", skipping...'
            )

    async def _get_proxy_for_request(self) -> tuple[ParseResult, ProxyInfo | None]:
        """Get the proxy URL and info for the next request.

        When `apifyProxySession` is configured, the proxy is issued through the configuration's confirmed
        session lease (the same lease shared with lazy password fetching and the access check). Otherwise the
        previous session-less rotation behavior is preserved.

        Raises:
            NotConfigured: If creation of the proxy configuration fails.

        Returns:
            Tuple of the parsed proxy URL and the related `ProxyInfo` (`None` for session-less rotation).
        """
        proxy_cfg = await self._get_proxy_configuration()

        session_id = self._proxy_settings.get('apifyProxySession')
        if session_id:
            proxy_info = await proxy_cfg.new_proxy_info(session_id=str(session_id))
            if proxy_info is None:
                Actor.log.error(
                    'Creation of proxy information failed. Check the field "proxyConfiguration" in the Actor input.'
                )
                raise NotConfigured
            return urlparse(proxy_info.url), proxy_info

        new_url = await proxy_cfg.new_url()
        return urlparse(str(new_url)), None

    async def _get_new_proxy_url(self) -> ParseResult:
        """Get a new proxy URL.

        Raises:
            NotConfigured: If creation of the proxy configuration fails.

        Returns: New proxy URL.
        """
        proxy_cfg = await self._get_proxy_configuration()

        # Get a new proxy URL and return it.
        new_url = await proxy_cfg.new_url()
        return urlparse(str(new_url))

    async def _get_proxy_configuration(self) -> ProxyConfiguration:
        """Get proxy configuration, creating it if necessary.

        Raises:
            NotConfigured: If creation of the proxy configuration fails.

        Returns: The shared proxy configuration used for every request.
        """
        # Get proxy configuration, creating it if necessary
        proxy_cfg = (
            self._proxy_cfg_internal
            if isinstance(self._proxy_cfg_internal, ProxyConfiguration)
            else await Actor.create_proxy_configuration(actor_proxy_input=self._proxy_settings)
        )

        # If the proxy configuration is still not available, raise an error. However, this should not happen due
        # to the checks in the `from_crawler` method.
        if proxy_cfg is None:
            Actor.log.error(
                'Creation of proxy configuration failed. Check the field "proxyConfiguration" in the Actor input.'
            )
            raise NotConfigured

        # Store the proxy configuration for future use
        self._proxy_cfg_internal = proxy_cfg
        return proxy_cfg
