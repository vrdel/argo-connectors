import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites

logger = Logger('test_feed.py')


class ParseServiceEndpointsTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_endpoint.xml') as feed_file:
            content = feed_file.read()
        logger.customer = 'CUSTOMERFOO'
        parse_service_endpoints = ParseServiceEndpoints(logger, content, 'CUSTOMERFOO', uid=True, pass_extensions=True)
        self.group_endpoints = parse_service_endpoints.get_group_endpoints()

    def test_parseserviceendpoints(self):
        pass


if __name__ == '__main__':
    unittest.main()
