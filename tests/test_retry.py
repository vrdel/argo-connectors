import unittest
import mock
import asyncio

from aiohttp import client_exceptions

from functools import wraps

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.log import Logger


logger = Logger('test_topofeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class async_test(object):
    """
    Decorator to create asyncio context for asyncio methods or functions.
    """
    def __init__(self, test_method):
        self.test_method = test_method

    def __call__(self, *args, **kwargs):
        test_obj = args[0]
        test_obj.loop.run_until_complete(self.test_method(*args, **kwargs))


class mockHttpGetEmpty(mock.AsyncMock):
    async def __aenter__(self, *args, **kwargs):
        mock_obj = mock.AsyncMock()
        mock_obj.text.return_value = ''
        return mock_obj
    async def __aexit__(self, *args, **kwargs):
        pass


class mockConnectionProblem(mock.AsyncMock):
    async def __aenter__(self, *args, **kwargs):
        mock_obj = mock.AsyncMock()
        mock_oserror = mock.create_autospec(OSError)
        mock_obj.text.side_effect = client_exceptions.ClientConnectorError('mocked key', mock_oserror)
        return mock_obj
    async def __aexit__(self, *args, **kwargs):
        pass


class ConnectorsHttpRetry(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
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
            'connectionretry': '3', 'connectionsleepretry': '1',
            'connectiontimeout': '180', 'generalpassextensions': 'True',
            'generalpublishwebapi': 'False', 'generalwriteavro': 'True',
            'inputstatedays': '3', 'inputstatesavedir':
            '/var/lib/argo-connectors/states/', 'outputweights':
            'weights_DATE.avro', 'webapihost': 'api.devel.argo.grnet.gr'
        }
        async def setsession():
            self.session = SessionWithRetry(logger, 'test_retry.py', self.globopts)
        self.loop.run_until_complete(setsession())

    # @unittest.skip("demonstrating skipping")
    @mock.patch('aiohttp_retry.RetryClient.get', side_effect=mockHttpGetEmpty)
    @async_test
    async def test_ConnectorEmptyRetry(self, mocked_get):
        path='/url_path'
        res = await self.session.http_get('{}://{}{}'.format('http', 'localhost', path))
        self.assertTrue(mocked_get.called)
        # defined in connectionretry
        self.assertEqual(mocked_get.call_count, 3)
        self.assertEqual(mocked_get.call_args[0][0], 'http://localhost/url_path')

    # @unittest.skip("demonstrating skipping")
    @mock.patch('aiohttp_retry.RetryClient.get', side_effect=mockConnectionProblem)
    @async_test
    async def test_ConnectorConnectionRetry(self, mocked_get):
        path='/url_path'
        res = await self.session.http_get('{}://{}{}'.format('http', 'localhost', path))
        self.assertTrue(mocked_get.called)
        # defined in connectionretry

    def tearDown(self):
        async def run():
            await self.session.close()
        self.loop.run_until_complete(run())
        self.loop.close()

