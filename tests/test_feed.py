import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites

logger = Logger('test_feed.py')

class ParseSitesTest(unittest.TestCase):
    def setUp(self):
        self.parse_sites = ParseSites(logger, data, 'FOO', uid=True, pass_extensions=True)

    def test_parsesites(self):
        pass


if __name__ == '__main__':
    unittest.main()
