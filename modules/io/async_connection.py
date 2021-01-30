#!/usr/bin/python3

import aiohttp
import aiohttp.client_exceptions
import asyncio
import time
import ssl
import xml.dom.minidom
from aiohttp_retry import RetryClient, ExponentialRetry, ListRetry


def build_ssl_settings(globopts):
    try:
        sslcontext = ssl.create_default_context(capath=globopts['AuthenticationCAPath'.lower()])
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
    def __init__(self, logger, msgprefix, globopts, custauth=None):
        self.ssl_context = build_ssl_settings(globopts)
        n_try, list_retry, client_timeout = build_connection_retry_settings(globopts)
        http_retry_options = ListRetry(timeouts=list_retry)
        client_timeout = aiohttp.ClientTimeout(total=client_timeout,
                                               connect=None, sock_connect=None,
                                               sock_read=None)
        self.session = RetryClient(retry_options=http_retry_options, timeout=client_timeout)
        self.n_try = n_try
        self.logger = logger
        if custauth:
            self.custauth = aiohttp.BasicAuth(
                custauth['AuthenticationHttpUser'.lower()],
                'AuthenticationHttpPass'.lower()
            )
        else:
            self.custauth = None

    async def _http_method(self, method, url, data=None, headers=None):
        method_obj = getattr(self.session, method)
        raised_exc = None
        n = 1
        try:
            while n <= self.n_try:
                try:
                    async with method_obj(url, data=data, headers=headers,
                                          ssl=self.ssl_context, auth=self.custauth) as response:
                        content = await response.text()
                        return content

                except Exception as exc:
                    self.logger.error('from http_{}() - {}'.format(method, repr(exc)))
                    raised_exc = exc

                self.logger.info(f'Connection try - {n}')
                n += 1

            else:
                self.logger.error('Connection retry exhausted')
                raise raised_exc

        except Exception as exc:
            # FIXME: correct logger messages
            raise exc

        finally:
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
