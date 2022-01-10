from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.log import Logger

from aiohttp import web
from functools import wraps

import asyncio
import unittest

logger = Logger('test_topofeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


def async_test(f):
    """
    Decorator to create asyncio context for asyncio methods or functions.
    """
    @wraps(f)
    def g(*args, **kwargs):
        args[0].loop.run_until_complete(f(*args, **kwargs))
    return g


class ConnectorsHttpRetry(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # you probably have some existing code above here
        cls.loop = asyncio.get_event_loop()

    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.globopts = {
            'authenticationcafile': '/etc/pki/tls/certs/ca-bundle.crt',
            'authenticationcapath': '/etc/grid-security/certificates',
            'authenticationhostcert': '/etc/grid-security/hostcert.pem',
            'authenticationhostkey': '/etc/grid-security/hostkey.pem',
            'authenticationhttppass': 'xxxx', 'authenticationhttpuser': 'xxxx',
            'authenticationuseplainhttpauth': 'False',
            'authenticationverifyservercert': 'True', 'avroschemasweights':
            '/etc/argo-egi-connectors/schemas//weight_sites.avsc',
            'connectionretry': '3', 'connectionsleepretry': '15',
            'connectiontimeout': '180', 'generalpassextensions': 'True',
            'generalpublishwebapi': 'False', 'generalwriteavro': 'True',
            'inputstatedays': '3', 'inputstatesavedir':
            '/var/lib/argo-connectors/states/', 'outputweights':
            'weights_DATE.avro', 'webapihost': 'api.devel.argo.grnet.gr'
        }
        async def setsession():
            self.session = SessionWithRetry(logger, 'test_retry.py', self.globopts)

        self.loop.run_until_complete(setsession())

    @async_test
    async def test_WeightsRetry(self):
        path='/vapor/downloadLavoisier/option/json/view/VAPOR_Ngi_Sites_Info'
        res = await self.session.http_get('{}://{}{}'.format('http', 'localhost', path))

    def tearDown(self):
        async def run():
            await self.session.close()
        self.loop.run_until_complete(run())
        self.loop.close()

