import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.gocdb_servicetypes import ParseGocdbServiceTypes

logger = Logger('test_servicetypefeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'

class ParseGocdb(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_types_gocdb.xml', encoding='utf-8') as feed_file:
            service_types = feed_file.read()
        logger.customer = CUSTOMER_NAME
        self.services_gocdb = ParseGocdbServiceTypes(logger, service_types)
        self.maxDiff = None

    def test_feedParse(self):
        self.services_gocdb.get_data()
