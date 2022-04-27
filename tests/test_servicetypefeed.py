import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.gocdb_servicetypes import ParseGocdbServiceTypes
from argo_egi_connectors.parse.flat_servicetypes import ParseFlatServiceTypes

logger = Logger('test_servicetypefeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'

class ParseGocdb(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_types_gocdb.xml', encoding='utf-8') as feed_file:
            service_types = feed_file.read()
        logger.customer = CUSTOMER_NAME
        self.services_gocdb = ParseGocdbServiceTypes(logger, service_types)
        self.maxDiff = None

    def test_GocdbFeedParse(self):
        service_types = self.services_gocdb.get_data()
        self.assertEqual(service_types, [
            {
                'description': 'Horizon is the canonical implementation of OpenStack’s '
                    'Dashboard, which provides a web based user interface to '
                    'OpenStack services',
                'name': 'org.openstack.horizon'
            },
            {
                'description': 'The primary endpoint for an OpenStack Cloud. Provides '
                    'identity and an endpoint catalog for other OpenStack '
                    'services',
                'name': 'org.openstack.keystone'
            },
            {
                'description': 'OpenStack Nova provides VM management services',
                'name': 'org.openstack.nova'
            },
            {
                'description': '',
                'name': 'service.type.empty.desc'
            }
        ])


class ParseFlat(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-topo.csv', encoding='utf-8') as feed_file:
            service_types = feed_file.read()
        logger.customer = CUSTOMER_NAME
        self.services_flat = ParseFlatServiceTypes(logger, service_types, is_csv=True)
        self.maxDiff = None

    def test_FlatFeedParse(self):
        service_types = self.services_flat.get_data()
        pass
