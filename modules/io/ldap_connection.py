import asyncio

import bonsai

from argo_egi_connectors.tools import module_class_name

def build_connection_retry_settings(globopts):
    retry = int(globopts['ConnectionRetry'.lower()])
    sleep_retry = int(globopts['ConnectionSleepRetry'.lower()])
    timeout = int(globopts['ConnectionTimeout'.lower()])
    list_retry = [(i + 1) * sleep_retry for i in range(retry)]
    return (retry, list_retry, timeout)

class ConnectorError(Exception):
    pass

class LDAPSessionWithRetry(object):
    def __init__(self, logger, globopts):
        self.n_try, self.sleep_retry_list, self.timeout = build_connection_retry_settings(globopts)
        self.logger = logger


    async def search(self, host, port, base, filter, attributes):
        raised_exc = None
        n = 1

        try:
            client = bonsai.LDAPClient('ldap://' + host + ':' + port + '/')
            while n <= self.n_try:
                try:
                    conn = await client.connect(True, timeout=float(self.timeout))
                    res = await conn.search(base,
                        bonsai.LDAPSearchScope.SUB, filter, attributes,
                        timeout=float(self.timeout))

                    print('Connection Successful')
                    return res


                except Exception as exc:
                    self.logger.error('from {}.search() - {}'.format(module_class_name(self), repr(exc)))
                    await asyncio.sleep(float(self.sleep_retry_list[n - 1]))
                    raised_exc = exc

                self.logger.info(f'Connection try - {n}')
                n += 1

            else:
                self.logger.error('Connection retry exhausted')
                raise raised_exc

        except Exception as exc:
            self.logger.error('from {}.search() - {}'.format(module_class_name(self), repr(exc)))
            raise ConnectorError()
