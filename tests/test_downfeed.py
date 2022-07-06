import unittest

from argo_connectors.log import Logger
from argo_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_connectors.parse.flat_topology import ParseFlatEndpoints
from argo_connectors.parse.provider_topology import ParseTopo, ParseExtensions, buildmap_id2groupname
from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.mesh.contacts import attach_contacts_topodata

logger = Logger('test_downfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseCsvDowntimes(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-downtimes.csv', encoding='utf-8') as feed_file:
            downtimes = feed_file.read()
        self.maxDiff = None

    def test_parseDowntimes(self):
        pass

if __name__ == '__main__':
    unittest.main()
