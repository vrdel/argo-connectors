import aiohttp
import asyncio
import time
import ssl
import xml.dom.minidom

from aiohttp_retry import RetryClient, ExponentialRetry, ListRetry
from argo_egi_connectors.utils import module_class_name
from argo_egi_connectors.exceptions import ConnectorHttpError, ConnectorParseError


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
            sleepsecs = float(self.globopts['ConnectionSleepRetry'.lower()])

            while n <= self.n_try:
                if n > 1:
                    if getattr(self.logger, 'job', False):
                        self.logger.info(f"{module_class_name(self)} Customer:{self.logger.customer} Job:{self.logger.job} : HTTP Connection try - {n} after sleep {sleepsecs} seconds")
                    else:
                        self.logger.info(f"{module_class_name(self)} Customer:{self.logger.customer} : HTTP Connection try - {n} after sleep {sleepsecs} seconds")
                try:
                    async with method_obj(url, data=data, headers=headers,
                                          ssl=self.ssl_context, auth=self.custauth) as response:
                        content = await response.text()
                        if content:
                            if self.verbose_ret:
                                return (content, response.headers, response.status)
                            else:
                                return content
                        else:
                            if getattr(self.logger, 'job', False):
                                self.logger.warn("{} Customer:{} Job:{} : HTTP Empty response".format(module_class_name(self),
                                                                                                      self.logger.customer, self.logger.job))
                            else:
                                self.logger.warn("{} Customer:{} : HTTP Empty response".format(module_class_name(self),
                                                                                               self.logger.customer))

                # do not retry on SSL errors
                except ssl.SSLError as exc:
                    raise exc

                except Exception as exc:
                    if getattr(self.logger, 'job', False):
                        self.logger.error('{}.http_{}({}) Customer:{} Job:{} - {}'.format(module_class_name(self),
                                                                                          method, url, self.logger.customer,
                                                                                          self.logger.job, repr(exc)))
                    else:
                        self.logger.error('{}.http_{}({}) Customer:{} - {}'.format(module_class_name(self),
                                                                                   method, url, self.logger.customer,
                                                                                   repr(exc)))
                    raised_exc = exc

                await asyncio.sleep(sleepsecs)
                n += 1

            else:
                if getattr(self.logger, 'job', False):
                    self.logger.info("{} Customer:{} Job:{} : HTTP Connection retry exhausted".format(module_class_name(self),
                                                                                                       self.logger.customer, self.logger.job))
                else:
                    self.logger.info("{} Customer:{} : HTTP Connection retry exhausted".format(module_class_name(self),
                                                                                               self.logger.customer))

        except Exception as exc:
            if getattr(self.logger, 'job', False):
                self.logger.error('{}.http_{}({}) Customer:{} Job:{} - {}'.format(module_class_name(self),
                                                                                  method, url, self.logger.customer,
                                                                                  self.logger.job, repr(exc)))
            else:
                self.logger.error('{}.http_{}({}) Customer:{} - {}'.format(module_class_name(self),
                                                                           method, url, self.logger.customer,
                                                                           repr(exc)))
            raise ConnectorHttpError()

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
