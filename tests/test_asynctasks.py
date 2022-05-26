import unittest

import unittest
import asyncio
import datetime

import mock

from argo_connectors.exceptions import ConnectorParseError, ConnectorHttpError
from argo_connectors.tasks.gocdb_servicetypes import TaskGocdbServiceTypes
from argo_connectors.tasks.provider_topology import TaskProviderTopology
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology
from argo_connectors.tasks.flat_servicetypes import TaskFlatServiceTypes

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


class TopologyGocdb(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        globopts = mock.Mock()
        webapiopts = mock.Mock()
        authopts = mock.Mock()
        bdiiopts = mock.Mock()
        confcust = mock.Mock()
        confcust.get_topofeedservicegroups.return_value = 'http://topo.feed.providers.com'
        confcust.get_topofeedendpoints.return_value = 'http://topo.feed.resources.com'
        topofeedpaging = False
        uidservendp = False
        fixed_date = datetime.datetime.now().strftime('%Y_%m_%d')
        fetchtype = 'ServiceGroups'
        self.topo_gocdb = TaskGocdbTopology(
            self.loop,
            logger,
            'test_asynctasks_topologygocdb',
            'https://gocdb.com/site-contacts',
            'https://gocdb.com/servicegroups-contacts',
            'https://gocdb.com/serviceendpoints_api',
            'https://gocdb.com/serviceegroups_api',
            'https://gocdb.com/sites_api',
            globopts,
            authopts,
            webapiopts,
            bdiiopts,
            confcust,
            CUSTOMER_NAME
            topofeedpaging,
            uidservendp,
            fixed_date,
            fetchtype
        )

    @mock.patch('argo_connectors.tasks.gocdb_topology.find_next_paging_cursor_count')
    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.gocdb_topology.SessionWithRetry.http_get')
    @async_test
    async def test_StepsFailedRun(self, mock_httpget, mock_buildsslsettings,
                                  mock_buildconnretry, mock_findpagingcursor):
        mock_buildsslsettings.return_value = 'ssl settings'
        mock_buildconnretry.return_value = (1, 2)
        mock_findpagingcursor.return_value = lambda: [0, 0, 0]
        await self.topo_gocdb.run()


class TopologyProvider(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        globopts = mock.Mock()
        webapiopts = mock.Mock()
        confcust = mock.Mock()
        confcust.get_topofeedservicegroups.return_value = 'http://topo.feed.providers.com'
        confcust.get_topofeedendpoints.return_value = 'http://topo.feed.resources.com'
        topofeedpaging = False
        uidservendp = False
        fixed_date = datetime.datetime.now().strftime('%Y_%m_%d')
        fetchtype = 'ServiceGroups'
        self.topo_provider = TaskProviderTopology(
            self.loop,
            logger,
            'test_asynctasks_topologyprovider',
            globopts,
            webapiopts,
            confcust,
            topofeedpaging,
            uidservendp,
            fixed_date,
            fetchtype
        )

    @mock.patch('argo_connectors.tasks.provider_topology.find_next_paging_cursor_count')
    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_get')
    @async_test
    async def test_StepsFailedRun(self, mock_httpget, mock_buildsslsettings,
                                  mock_buildconnretry, mock_findpagingcursor):
        mock_buildsslsettings.return_value = 'ssl settings'
        mock_buildconnretry.return_value = (1, 2)
        mock_findpagingcursor.return_value = lambda: [0, 0, 0]
        await self.topo_provider.run()


class ServiceTypesGocdb(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        mocked_globopts = dict(generalpublishwebapi='True')
        globopts = mocked_globopts
        webapiopts = mock.Mock()
        authopts = mock.Mock()
        confcust = mock.Mock()
        custname = CUSTOMER_NAME
        feed = 'https://service-types.com/api/fetch'
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
        self.services_gocdb = TaskGocdbServiceTypes(
            self.loop,
            logger,
            'test_asynctasks_servicetypesgocdb',
            globopts,
            authopts,
            webapiopts,
            confcust,
            custname,
            feed,
            timestamp
        )
        self.maxDiff = None

    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate):
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_data.side_effect = ['data_servicetypes']
        self.services_gocdb.send_webapi = mock.AsyncMock()
        self.services_gocdb.parse_source = mock.MagicMock()
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.fetch_data.called)
        self.assertTrue(self.services_gocdb.parse_source.called)
        self.services_gocdb.parse_source.assert_called_with('data_servicetypes')
        self.assertEqual(mock_writestate.call_args[0][0], 'test_asynctasks_servicetypesgocdb')
        self.assertEqual(mock_writestate.call_args[0][3], self.services_gocdb.timestamp)
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_gocdb.send_webapi.called)
        self.assertTrue(self.services_gocdb.logger.info.called)

    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate):
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_data.side_effect = [ConnectorHttpError('fetch_data failed')]
        self.services_gocdb.send_webapi = mock.AsyncMock()
        self.services_gocdb.parse_source = mock.MagicMock()
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.fetch_data.called)
        self.assertFalse(self.services_gocdb.parse_source.called)
        self.assertEqual(mock_writestate.call_args[0][0], 'test_asynctasks_servicetypesgocdb')
        self.assertEqual(mock_writestate.call_args[0][3], self.services_gocdb.timestamp)
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_gocdb.logger.error.called)
        self.assertTrue(self.services_gocdb.logger.error.call_args[0][0], repr(ConnectorHttpError('fetch_data failed')))
        self.assertFalse(self.services_gocdb.send_webapi.called)


class ServiceTypesFlat(unittest.TestCase):
    def setUp(self):
        logger = mock.Mock()
        logger.customer = CUSTOMER_NAME
        self.loop = asyncio.get_event_loop()
        mocked_globopts = dict(generalpublishwebapi='True')
        globopts = mocked_globopts
        webapiopts = mock.Mock()
        authopts = mock.Mock()
        confcust = mock.Mock()
        custname = CUSTOMER_NAME
        feed = 'https://service-types.com/api/fetch'
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
        self.services_flat = TaskFlatServiceTypes(
            self.loop,
            logger,
            'test_asynctasks_servicetypesflat',
            globopts,
            authopts,
            webapiopts,
            confcust,
            custname,
            feed,
            timestamp
        )
        self.maxDiff = None

    @mock.patch('argo_connectors.tasks.flat_servicetypes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate):
        self.services_flat.fetch_data = mock.AsyncMock()
        self.services_flat.fetch_data.side_effect = ['data_servicetypes']
        self.services_flat.send_webapi = mock.AsyncMock()
        self.services_flat.parse_source = mock.MagicMock()
        await self.services_flat.run()
        self.assertTrue(self.services_flat.fetch_data.called)
        self.assertTrue(self.services_flat.parse_source.called)
        self.services_flat.parse_source.assert_called_with('data_servicetypes')
        self.assertEqual(mock_writestate.call_args[0][0], 'test_asynctasks_servicetypesflat')
        self.assertEqual(mock_writestate.call_args[0][3], self.services_flat.timestamp)
        self.assertTrue(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_flat.send_webapi.called)
        self.assertTrue(self.services_flat.logger.info.called)

    @mock.patch('argo_connectors.tasks.flat_servicetypes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate):
        self.services_flat.fetch_data = mock.AsyncMock()
        self.services_flat.fetch_data.side_effect = [ConnectorHttpError('fetch_data failed')]
        self.services_flat.send_webapi = mock.AsyncMock()
        self.services_flat.parse_source = mock.MagicMock()
        await self.services_flat.run()
        self.assertTrue(self.services_flat.fetch_data.called)
        self.assertFalse(self.services_flat.parse_source.called)
        self.assertEqual(mock_writestate.call_args[0][0], 'test_asynctasks_servicetypesflat')
        self.assertEqual(mock_writestate.call_args[0][3], self.services_flat.timestamp)
        self.assertFalse(mock_writestate.call_args[0][4])
        self.assertTrue(self.services_flat.logger.error.called)
        self.assertTrue(self.services_flat.logger.error.call_args[0][0], repr(ConnectorHttpError('fetch_data failed')))
        self.assertFalse(self.services_flat.send_webapi.called)
