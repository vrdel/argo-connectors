import unittest

import unittest
import asyncio
import datetime

import mock

from argo_egi_connectors.exceptions import ConnectorParseError, ConnectorHttpError
from argo_egi_connectors.tasks.gocdb_servicetypes import TaskGocdbServiceTypes

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


class ServiceTypesGocdb(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        globopts = mock.Mock()
        webapiopts = mock.Mock()
        confcust = mock.Mock()
        custname = CUSTOMER_NAME
        feed = 'https://service-types.com/api/fetch'
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
        self.services_gocdb = TaskGocdbServiceTypes(
            self.loop,
            logger,
            'test_asynctasks',
            globopts,
            webapiopts,
            confcust,
            custname,
            feed,
            timestamp
        )
        self.maxDiff = None

    @mock.patch('argo_egi_connectors.tasks.gocdb_servicetypes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate):
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_data.side_effect = ['data_servicetypes']
        self.services_gocdb.parse_source = mock.Mock()
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.fetch_data.called)
        self.assertTrue(self.services_gocdb.parse_source.called)
        self.services_gocdb.parse_source.assert_called_with('data_servicetypes')
        self.assertEqual(mock_writestate.call_args[0][0], 'test_asynctasks')
        self.assertEqual(mock_writestate.call_args[0][3], self.services_gocdb.timestamp)
        self.assertTrue(mock_writestate.call_args[0][4])

    @mock.patch('argo_egi_connectors.tasks.gocdb_servicetypes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate):
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_data.side_effect = [ConnectorHttpError('fetch_data failed')]
        self.services_gocdb.parse_source = mock.Mock()
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.fetch_data.called)
        self.assertFalse(self.services_gocdb.parse_source.called)
        self.assertEqual(mock_writestate.call_args[0][0], 'test_asynctasks')
        self.assertEqual(mock_writestate.call_args[0][3], self.services_gocdb.timestamp)
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_gocdb.logger.error.called)
        self.assertTrue(self.services_gocdb.logger.error.call_args[0][0], repr(ConnectorHttpError('fetch_data failed')))
