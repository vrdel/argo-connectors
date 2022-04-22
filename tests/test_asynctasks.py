import unittest

import unittest
import asyncio
import datetime

import mock

from argo_egi_connectors.log import Logger
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.tasks.gocdb_servicetypes import TaskGocdbServiceTypes

logger = Logger('test_asynctasks.py')
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
    async def test_StepsRun(self, mock_writestate):
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.parse_source = mock.Mock()
        self.services_gocdb.fetch_data.side_effect = ['data_servicetypes']
        await self.services_gocdb.run()
        self.assertEqual(self.services_gocdb.fetch_data.called, True)
        self.assertEqual(self.services_gocdb.parse_source.called, True)
        self.services_gocdb.parse_source.assert_called_with('data_servicetypes')
        self.assertEqual(mock_writestate.call_args[0][0], 'test_asynctasks')
        self.assertEqual(mock_writestate.call_args[0][3], self.services_gocdb.timestamp)
        self.assertEqual(mock_writestate.call_args[0][4], True)
