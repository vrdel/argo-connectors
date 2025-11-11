import unittest

import unittest
import asyncio
import datetime
import json

import mock

from argo_connectors.config.glob import Global
from argo_connectors.log import Logger
from argo_connectors.config.customer import Customer
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.tasks.flat_downtimes import TaskCsvDowntimes
from argo_connectors.tasks.flat_servicetypes import TaskFlatServiceTypes
from argo_connectors.tasks.gocdb_servicetypes import TaskGocdbServiceTypes
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology, find_next_paging_cursor_count
from argo_connectors.tasks.provider_topology import TaskProviderTopology
from argo_connectors.parse.base import ParseHelpers


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
        globopts = mock.MagicMock()
        webapiopts = mock.MagicMock()
        authopts = mock.MagicMock()
        bdiiopts = mock.MagicMock()
        bdiiopts.__getitem__.return_value = 'True'
        confcust = mock.Mock()
        topofeedpaging = True
        notification_flag = True
        uidservendp = False
        passext = True
        fixed_date = datetime.datetime.now().strftime('%Y_%m_%d')
        fetchtype = 'ServiceGroups'
        self.topo_gocdb = TaskGocdbTopology(
            self.loop,
            logger,
            'test_asynctasks_topologygocdb',
            'https://gocdb.com/serviceendpoints_api',
            'https://gocdb.com/serviceegroups_api',
            'https://gocdb.com/sites_api',
            globopts,
            authopts,
            webapiopts,
            bdiiopts,
            confcust,
            CUSTOMER_NAME,
            'https://gocdb.com/',
            fetchtype,
            fixed_date,
            uidservendp,
            passext,
            topofeedpaging,
            notification_flag
        )

    @mock.patch.object(ParseHelpers, 'parse_xml')
    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.gocdb_topology.TaskGocdbTopology.fetch_ldap_data')
    @mock.patch('argo_connectors.tasks.gocdb_topology.SessionWithRetry.http_get')
    @async_test
    async def test_failedNextCursor(self, mock_httpget, mock_fetchldap,
                                    mock_buildsslsettings,
                                    mock_buildconnretry, mock_parsexml):
        mock_httpget.return_value = 'garbled XML data'
        mock_buildsslsettings.return_value = 'SSL settings'
        mock_parsexml.side_effect = [
            ConnectorParseError('failed GOCDB find_next_paging_cursor_count'),
            ConnectorParseError('failed GOCDB find_next_paging_cursor_count'),
            ConnectorParseError('failed GOCDB find_next_paging_cursor_count')
        ]
        mock_buildconnretry.return_value = (1, 2)
        with self.assertRaises(ConnectorError) as cm:
            await self.topo_gocdb.run()
        excep = cm.exception
        self.assertTrue('ConnectorParseError' in excep.msg)
        self.assertTrue('failed GOCDB' in excep.msg)


class TestFindNextPagingCursorCount(unittest.TestCase):
    def setUp(self):
        _ = Logger(f'{__name__}.{__class__.__name__}')
        with open('tests/sample-topofeedpaging.xml') as tf:
            self.res = tf.read()

    def test_count_n_cursor(self):
        paging = find_next_paging_cursor_count(self.res)
        count, cursor = paging()

        self.assertEqual(count, 95)
        self.assertEqual(cursor, '134')


class TopologyProvider(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        _ = Global('topology-provider-connector.py')
        _ = Customer('topology-provider-connector.py')
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = CUSTOMER_NAME
        fixed_date = datetime.datetime.now().strftime('%Y_%m_%d')
        self.topo_provider = TaskProviderTopology(
            fixed_date
        )
        self.topo_provider.Customer.custopts['TopoFeedServiceGroups'] = 'http://topo.feed.providers.com'
        self.topo_provider.Customer.custopts['TopoFeedEndpoints'] = 'http://topo.feed.resources.com'
        self.topo_provider.Customer.custopts['OIDCRefreshToken'] = 'oidctoken'
        self.topo_provider.Customer.custopts['OIDCClientId'] = 'clientid'
        self.topo_provider.Customer.custopts['OIDCTokenEndpoint'] = 'oidctokenapi'

    @mock.patch.object(ParseHelpers, 'parse_json')
    @mock.patch('argo_connectors.tasks.provider_topology.TaskProviderTopology.token_fetch')
    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_post')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_get')
    @async_test
    async def test_failedNextCursor(self, mock_httpget, mock_httppost, mock_buildsslsettings,
                                    mock_buildconnretry, mock_tokenfetch, mock_parsejson):
        mock_httpget.return_value = 'garbled JSON data'
        mock_httppost.return_value = 'garbled JSON data'
        mock_buildsslsettings.return_value = 'SSL settings'
        self.topo_provider.store_refresh_token = mock.Mock()
        mock_tokenfetch.return_value = ('OIDC Access token', 'OIDC Refresh token')
        mock_parsejson.side_effect = [
            ConnectorParseError(
                'failed PROVIDER find_next_paging_cursor_count'),
            ConnectorParseError(
                'failed PROVIDER find_next_paging_cursor_count'),
        ]
        mock_buildconnretry.return_value = (1, 2)
        with self.assertRaises(ConnectorError) as cm:
            await self.topo_provider.run()
        excep = cm.exception
        self.assertTrue(type(excep), ConnectorParseError)
        self.assertTrue('failed PROVIDER' in excep.msg)

    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_post')
    @mock.patch.object(TaskProviderTopology, 'fetch_data')
    @async_test
    async def test_failedAccessToken(self, mock_fetchdata, mock_httppost,
                                     mock_buildsslsettings,
                                     mock_buildconnretry):
        mock_fetchdata.return_value = 'OK JSON data'
        mock_httppost.return_value = 'garbled JSON data'
        mock_buildsslsettings.return_value = 'SSL settings'
        mock_buildconnretry.return_value = (1, 2)
        with self.assertRaises(ConnectorError) as cm:
            await self.topo_provider.run()
        excep = cm.exception
        self.assertTrue(type(excep), ConnectorParseError)
        self.assertTrue('JSONDecodeError' in excep.msg)

    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_post')
    @mock.patch.object(TaskProviderTopology, 'fetch_data')
    @async_test
    async def test_failedAccessToken2(self, mock_fetchdata, mock_httppost,
                                      mock_buildsslsettings,
                                      mock_buildconnretry):
        mock_fetchdata.return_value = 'OK JSON data'
        mock_httppost.return_value = "{\"no\": \"access_token\"}"
        mock_buildsslsettings.return_value = 'SSL settings'
        mock_buildconnretry.return_value = (1, 2)
        with self.assertRaises(ConnectorError) as cm:
            await self.topo_provider.run()
        excep = cm.exception
        self.assertTrue(type(excep), ConnectorParseError)
        self.assertTrue('Could not extract OIDC Access token' in excep.msg)

    @mock.patch('argo_connectors.tasks.provider_topology.write_json')
    @mock.patch('argo_connectors.tasks.provider_topology.write_state')
    @mock.patch('argo_connectors.tasks.provider_topology.buildmap_id2groupname')
    @mock.patch('argo_connectors.tasks.provider_topology.ParseResourcesContacts')
    @mock.patch('argo_connectors.tasks.provider_topology.attach_contacts_topodata')
    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_post')
    @mock.patch.object(TaskProviderTopology, 'fetch_data')
    @async_test
    async def test_RefreshTokenStore(self, mock_fetchdata, mock_httppost,
                                     mock_buildsslsettings,
                                     mock_buildconnretry, mock_attachcontacts,
                                     mock_parseresourcecontacts,
                                     mock_buildmapid2group, mock_writestate,
                                     mock_writejson):
        mock_fetchdata.return_value = 'OK JSON data'
        mock_httppost.return_value = json.dumps(
            {
                'refresh_token': 'NEW_REFRESH_TOKEN',
                'access_token': 'ACCESS_TOKEN'
            }
        )
        mock_buildsslsettings.return_value = 'SSL settings'
        mock_buildconnretry.return_value = (1, 2)
        self.topo_provider.parse_source_topo = mock.Mock()
        self.topo_provider.parse_source_topo.return_value = ['group_groups', 'group_endpoints']
        self.topo_provider.store_refresh_token = mock.Mock()
        self.topo_provider.parse_source_extensions = mock.MagicMock()
        await self.topo_provider.run()
        self.assertTrue(self.topo_provider.store_refresh_token.called)
        self.topo_provider.store_refresh_token.assert_called_with('oidctoken', 'NEW_REFRESH_TOKEN')

    @mock.patch('argo_connectors.tasks.provider_topology.write_json')
    @mock.patch('argo_connectors.tasks.provider_topology.json.loads')
    @mock.patch('argo_connectors.tasks.provider_topology.open')
    @mock.patch('argo_connectors.tasks.provider_topology.os.path.exists')
    @mock.patch('argo_connectors.tasks.provider_topology.write_state')
    @mock.patch('argo_connectors.tasks.provider_topology.buildmap_id2groupname')
    @mock.patch('argo_connectors.tasks.provider_topology.ParseResourcesContacts')
    @mock.patch('argo_connectors.tasks.provider_topology.attach_contacts_topodata')
    @mock.patch('argo_connectors.io.http.build_connection_retry_settings')
    @mock.patch('argo_connectors.io.http.build_ssl_settings')
    @mock.patch('argo_connectors.tasks.provider_topology.SessionWithRetry.http_post')
    @mock.patch.object(TaskProviderTopology, 'fetch_data')
    @async_test
    async def test_RefreshTokenRead(self, mock_fetchdata, mock_httppost,
                                    mock_buildsslsettings, mock_buildconnretry,
                                    mock_attachcontacts,
                                    mock_parseresourcecontacts,
                                    mock_buildmapid2group, mock_writestate,
                                    mock_pathexists, mock_open, mock_jsonloads,
                                    mock_writejson):
        mock_fetchdata.return_value = 'OK JSON data'
        mock_jsonloads.return_value = {
            'prev': 'PREVIOUS_REFRESH_TOKEN',
            'next': 'NEXT_REFRESH_TOKEN'
        }
        mock_buildsslsettings.return_value = 'SSL settings'
        mock_buildconnretry.return_value = (1, 2)
        self.topo_provider.parse_source_topo = mock.Mock()
        self.topo_provider.parse_source_topo.return_value = ['group_groups', 'group_endpoints']
        self.topo_provider.store_refresh_token = mock.Mock()
        self.topo_provider.parse_source_extensions = mock.MagicMock()
        self.topo_provider.token_fetch = mock.AsyncMock()
        self.topo_provider.token_fetch.return_value = ('ACCESS_TOKEN', 'REFRESH_TOKEN')
        await self.topo_provider.run()
        self.topo_provider.token_fetch.assert_called_with('clientid', 'NEXT_REFRESH_TOKEN', 'oidctokenapi')


class ServiceTypesGocdb(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        _ = Global('service-types-gocdb-connector.py')
        _ = Customer('service-types-gocdb-connector.py')
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = CUSTOMER_NAME
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
        self.services_gocdb = TaskGocdbServiceTypes(
            timestamp,
        )
        self.services_gocdb.globopts['GeneralPublishWebAPI'.lower()] = True
        self.maxDiff = None

    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.WebAPI')
    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.write_json')
    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate, mock_writejson, mock_webapi):
        service_type_1 = {
            'name': 'service.type.1',
            'description': 'description 1',
            'tags': ['topology']

        }
        service_type_2 = {
            'name': 'service.type.2',
            'description': 'description 2',
            'tags': ['poem']
        }
        web_api = mock_webapi.return_value
        web_api.get = mock.AsyncMock()
        web_api.send = mock.AsyncMock()
        web_api.session = mock.AsyncMock()
        web_api.get.return_value = [service_type_2]
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_data.side_effect = [service_type_1]
        self.services_gocdb.parse_source = mock.MagicMock()
        self.services_gocdb.parse_source.return_value = [service_type_1]
        self.services_gocdb.parse_webapi_poem = mock.MagicMock()
        self.services_gocdb.parse_webapi_poem.return_value = [service_type_2]
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.fetch_data.called)
        self.assertTrue(self.services_gocdb.parse_source.called)
        self.services_gocdb.parse_source.assert_called_with(service_type_1)
        self.assertEqual(mock_writestate.call_args[0][0],
                         self.services_gocdb.fixed_date)
        self.assertTrue(mock_writestate.call_args[0][1])
        self.assertTrue(web_api.send.called)
        self.assertTrue(mock_writejson.called)
        self.assertEqual(
            mock_writejson.call_args[0][0], [service_type_1, service_type_2]
        )

    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.WebAPI')
    @mock.patch('argo_connectors.tasks.gocdb_servicetypes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate, mock_webapi):
        web_api = mock_webapi.return_value
        web_api.get = mock.AsyncMock()
        web_api.send = mock.AsyncMock()
        web_api.session = mock.AsyncMock()
        self.services_gocdb.fetch_data = mock.AsyncMock()
        self.services_gocdb.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')
        ]
        self.services_gocdb.send_webapi = mock.AsyncMock()
        self.services_gocdb.parse_source = mock.MagicMock()
        self.services_gocdb.fetch_webapi = mock.AsyncMock()
        self.services_gocdb.fetch_webapi.side_effect = ['data_webapi_servicetypes']
        await self.services_gocdb.run()
        self.assertTrue(self.services_gocdb.fetch_data.called)
        self.assertFalse(self.services_gocdb.parse_source.called)
        self.assertEqual(mock_writestate.call_args[0][0],
                         self.services_gocdb.fixed_date)
        self.assertFalse(mock_writestate.call_args[0][1])
        self.assertFalse(self.services_gocdb.send_webapi.called)


class ServiceTypesFlat(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        _ = Global('service-types-csv-connector.py')
        _ = Customer('service-types-csv-connector.py')
        logger = Logger(f'{__name__}.{__class__.__name__}')
        logger.customer = CUSTOMER_NAME
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
        self.services_flat = TaskFlatServiceTypes(
            timestamp
        )
        self.services_flat.globopts['GeneralPublishWebAPI'.lower()] = True
        self.maxDiff = None

    @mock.patch('argo_connectors.tasks.flat_servicetypes.WebAPI')
    @mock.patch('argo_connectors.tasks.flat_servicetypes.write_json')
    @mock.patch('argo_connectors.tasks.flat_servicetypes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate, mock_writejson, mock_webapi):
        service_type_1 = {
            'name': 'service.type.1',
            'description': 'description 1',
            'tags': []

        }
        service_type_2 = {
            'name': 'service.type.2',
            'description': 'description 2',
            'tags': []
        }
        web_api = mock_webapi.return_value
        web_api.get = mock.AsyncMock()
        web_api.send = mock.AsyncMock()
        web_api.session = mock.AsyncMock()
        web_api.get.return_value = [service_type_2]
        self.services_flat.fetch_data = mock.AsyncMock()
        self.services_flat.fetch_data.side_effect = [service_type_1]
        self.services_flat.parse_source = mock.MagicMock()
        self.services_flat.parse_source.return_value = [service_type_1]
        self.services_flat.parse_webapi_poem = mock.MagicMock()
        self.services_flat.parse_webapi_poem.return_value = [service_type_2]
        await self.services_flat.run()
        self.assertTrue(self.services_flat.fetch_data.called)
        self.assertTrue(self.services_flat.parse_source.called)
        self.services_flat.parse_source.assert_called_with(service_type_1)
        self.assertEqual(mock_writestate.call_args[0][0],
                         self.services_flat.fixed_date)
        self.assertTrue(mock_writestate.call_args[0][1])
        self.assertTrue(web_api.send.called)
        self.assertTrue(mock_writejson.called)
        self.assertEqual(
            mock_writejson.call_args[0][0], [service_type_1, service_type_2]
        )

    @mock.patch('argo_connectors.tasks.flat_servicetypes.WebAPI')
    @mock.patch('argo_connectors.tasks.flat_servicetypes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate, mock_webapi):
        web_api = mock_webapi.return_value
        web_api.get = mock.AsyncMock()
        web_api.send = mock.AsyncMock()
        web_api.session = mock.AsyncMock()
        self.services_flat.fetch_data = mock.AsyncMock()
        self.services_flat.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')
        ]
        self.services_flat.send_webapi = mock.AsyncMock()
        self.services_flat.parse_source = mock.MagicMock()
        self.services_flat.fetch_webapi = mock.AsyncMock()
        self.services_flat.fetch_webapi.side_effect = ['data_webapi_servicetypes']
        await self.services_flat.run()
        self.assertTrue(self.services_flat.fetch_data.called)
        self.assertFalse(self.services_flat.parse_source.called)
        self.assertEqual(mock_writestate.call_args[0][0],
                         self.services_flat.fixed_date)
        self.assertFalse(mock_writestate.call_args[0][1])
        self.assertFalse(self.services_flat.send_webapi.called)


class DowntimesCsv(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()
        _ = Global('downtimes-csv-connector.py')
        _ = Customer('downtimes-csv-connector.py')
        _ = Logger(f'{__name__}.{__class__.__name__}')
        timestamp = datetime.datetime.now().strftime('%Y_%m_%d')
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')
        self.downtimes_flat = TaskCsvDowntimes(
            current_date,
            current_date,
            timestamp,
            combuid=None
        )
        self.downtimes_flat.globopts['GeneralPublishWebAPI'.lower()] = True
        self.maxDiff = None

    @mock.patch('argo_connectors.tasks.flat_downtimes.WebAPI')
    @mock.patch('argo_connectors.tasks.flat_downtimes.write_json')
    @mock.patch('argo_connectors.tasks.flat_downtimes.write_state')
    @async_test
    async def test_StepsSuccessRun(self, mock_writestate, mock_writejson, mock_webapi):
        web_api = mock_webapi.return_value
        web_api.send = mock.AsyncMock()
        web_api.session = mock.AsyncMock()
        self.downtimes_flat.fetch_data = mock.AsyncMock()
        self.downtimes_flat.fetch_data.side_effect = ['data_downtimes']
        self.downtimes_flat.send_webapi = mock.AsyncMock()
        self.downtimes_flat.parse_source = mock.MagicMock()
        await self.downtimes_flat.run()
        self.assertTrue(self.downtimes_flat.fetch_data.called)
        self.assertTrue(self.downtimes_flat.parse_source.called)
        self.downtimes_flat.parse_source.assert_called_with('data_downtimes')
        self.assertEqual(
            mock_writestate.call_args[0][0], self.downtimes_flat.timestamp)
        self.assertTrue(mock_writestate.call_args[0][1])
        self.assertTrue(mock_writejson.called, True)
        self.assertEqual(
            mock_writejson.call_args[0][1], datetime.datetime.now().strftime('%Y_%m_%d'))
        self.assertTrue(web_api.send.called)

    @mock.patch('argo_connectors.tasks.flat_downtimes.WebAPI')
    @mock.patch('argo_connectors.tasks.flat_downtimes.write_state')
    @async_test
    async def test_StepsFailedRun(self, mock_writestate, mock_webapi):
        web_api = mock_webapi.return_value
        web_api.send = mock.AsyncMock()
        web_api.session = mock.AsyncMock()
        self.downtimes_flat.fetch_data = mock.AsyncMock()
        self.downtimes_flat.fetch_data.side_effect = [
            ConnectorHttpError('fetch_data failed')]
        self.downtimes_flat.send_webapi = mock.AsyncMock()
        self.downtimes_flat.parse_source = mock.MagicMock()
        await self.downtimes_flat.run()
        self.assertTrue(self.downtimes_flat.fetch_data.called)
        self.assertFalse(self.downtimes_flat.parse_source.called)
        self.assertEqual(
            mock_writestate.call_args[0][0], self.downtimes_flat.timestamp)
        self.assertFalse(mock_writestate.call_args[0][1])
        self.assertFalse(web_api.send.called)
