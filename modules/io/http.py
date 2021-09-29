import aiohttp
import aiohttp.client_exceptions
import asyncio
import time
import ssl
import xml.dom.minidom

from aiohttp_retry import RetryClient, ExponentialRetry, ListRetry
from argo_egi_connectors.utils import module_class_name


def build_ssl_settings(globopts):
    try:
        sslcontext = ssl.create_default_context(capath=globopts['AuthenticationCAPath'.lower()],
                                                cafile=globopts['AuthenticationCAFile'.lower()])
        sslcontext.load_cert_chain(globopts['AuthenticationHostCert'.lower()],
                                   globopts['AuthenticationHostKey'.lower()])

        return sslcontext

    except KeyError:
        return None


def build_connection_retry_settings(globopts):
    retry = int(globopts['ConnectionRetry'.lower()])
    sleep_retry = int(globopts['ConnectionSleepRetry'.lower()])
    timeout = int(globopts['ConnectionTimeout'.lower()])
    list_retry = [(i + 1) * sleep_retry for i in range(retry)]
    return (retry, list_retry, timeout)


class ConnectorError(Exception):
    pass


class SessionWithRetry(object):
    def __init__(self, logger, msgprefix, globopts, token=None, custauth=None,
                 verbose_ret=False, handle_session_close=False):
        self.ssl_context = build_ssl_settings(globopts)
        n_try, list_retry, client_timeout = build_connection_retry_settings(globopts)
        http_retry_options = ListRetry(timeouts=list_retry)
        client_timeout = aiohttp.ClientTimeout(total=client_timeout,
                                               connect=None, sock_connect=None,
                                               sock_read=None)
        self.session = RetryClient(retry_options=http_retry_options, timeout=client_timeout)
        self.n_try = n_try
        self.logger = logger
        self.token = token
        if custauth:
            self.custauth = aiohttp.BasicAuth(
                custauth['AuthenticationHttpUser'.lower()],
                custauth['AuthenticationHttpPass'.lower()]
            )
        else:
            self.custauth = None
        self.verbose_ret = verbose_ret
        self.handle_session_close = handle_session_close
        self.globopts = globopts

    async def _http_method(self, method, url, data=None, headers=None):
        method_obj = getattr(self.session, method)
        raised_exc = None
        n = 1
        if self.token:
            headers = headers or {}
            headers.update({
                'x-api-key': self.token,
                'Accept': 'application/json'
            })
        try:
            while n <= self.n_try:
                try:
                    async with method_obj(url, data=data, headers=headers,
                                          ssl=self.ssl_context, auth=self.custauth) as response:
                        content = await response.text()
                        if self.verbose_ret:
                            return (content, response.headers, response.status)
                        else:
                            return content

                # do not retry on SSL errors, exit immediately
                except ssl.SSLError as exc:
                    raise exc

                except Exception as exc:
                    self.logger.error('from {}.http_{}() - {}'.format(module_class_name(self), method, repr(exc)))
                    await asyncio.sleep(float(self.globopts['ConnectionSleepRetry'.lower()]))
                    raised_exc = exc

                self.logger.info(f'Connection try - {n}')
                n += 1

            else:
                self.logger.error('Connection retry exhausted')
                raise raised_exc

        except Exception as exc:
            self.logger.error('from {}.http_{}() - {}'.format(module_class_name(self), method, repr(exc)))
            raise ConnectorError()

        finally:
            if not self.handle_session_close:
                await self.session.close()

    async def http_get(self, url, headers=None):
        try:
            content = await self._http_method('get', url, headers=headers)
            return content

        except Exception as exc:
            raise exc

    async def http_put(self, url, data, headers=None):
        try:
            content = await self._http_method('put', url, data=data,
                                              headers=headers)
            return content

        except Exception as exc:
            raise exc

    async def http_post(self, url, data, headers=None):
        try:
            content = await self._http_method('post', url, data=data,
                                              headers=headers)
            return content

        except Exception as exc:
            raise exc

    async def http_delete(self, url, headers=None):
        try:
            content = await self._http_method('delete', url, headers=headers)
            return content

        except Exception as exc:
            raise exc

    async def close(self):
        return await self.session.close()
