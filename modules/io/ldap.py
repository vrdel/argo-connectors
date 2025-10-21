import asyncio

import bonsai

from argo_connectors.utils import module_class_name
from argo_connectors.exceptions import ConnectorHttpError
from argo_connectors.log import Logger


class LDAPSessionWithRetry:
    def __init__(self, retry_attempts, retry_sleep, connection_timeout):
        self.n_try = retry_attempts
        self.retry_sleep_list = [(i + 1) * retry_sleep for i in range(retry_attempts)]
        self.timeout = connection_timeout

    async def search(self, host, port, base, filter, attributes):
        raised_exc = None
        n = 1

        try:
            client = bonsai.LDAPClient('ldap://' + host + ':' + port + '/')
            while n <= self.n_try:
                try:
                    conn = await client.connect(True, timeout=float(self.timeout))
                    res = await conn.search(base, bonsai.LDAPSearchScope.SUB,
                                            filter, attributes,
                                            timeout=float(self.timeout))

                    return res

                except Exception as exc:
                    Logger.error('from {}.search() - {}'.format(module_class_name(self), repr(exc)))
                    await asyncio.sleep(float(self.retry_sleep_list[n - 1]))
                    raised_exc = exc

                Logger.info(f'LDAP Connection try - {n}')
                n += 1

            else:
                Logger.error('LDAP Connection retry exhausted')

        except Exception as exc:
            Logger.error('from {}.search() - {}'.format(module_class_name(self), repr(exc)))
            raise ConnectorHttpError()
