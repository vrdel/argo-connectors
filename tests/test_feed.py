import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites, ConnectorError
#from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites

logger = Logger('test_feed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'

class ParseServiceEndpointsTest(unittest.TestCase):
    def setUp(self):
        with open('sample-service_endpoint.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_service_endpoints = ParseServiceEndpoints(logger, self.content, CUSTOMER_NAME)
        self.group_endpoints = parse_service_endpoints.get_group_endpoints()

    def endpoints_have_extension(group_endpoints):
        for gren in group_endpoints:
            for key in gren['tags'].keys():
                if key.startswith('info_ext_'):
                    return True
        return False

    def get_group(group_endpoints, group_name):
        for group in group_endpoints:
            if group['group'] == group_name:
                return group

        return None


    def test_parseserviceendpoints(self):
        self.assertEqual(len(self.group_endpoints), 3) # Parsed correct number of endpoint groups

        # Assertions for GSI_LCG2
        gsi_lcg2 = ParseServiceEndpointsTest.get_group(self.group_endpoints, 'GSI-LCG2')
        self.assertIsNotNone(gsi_lcg2)
        self.assertEqual(gsi_lcg2['service'], 'CE')
        self.assertEqual(gsi_lcg2['hostname'], 'grid13.gsi.de')
        gsi_lcg2_tags = gsi_lcg2['tags']
        self.assertEqual(gsi_lcg2_tags['scope'], 'EGI, wlcg, tier2, alice')
        self.assertEqual(gsi_lcg2_tags['monitored'], '0')
        self.assertEqual(gsi_lcg2_tags['production'], '0')

        # Assertions for RAL_LCG2
        ral_lcg2 = ParseServiceEndpointsTest.get_group(self.group_endpoints, 'RAL-LCG2')
        self.assertIsNotNone(ral_lcg2)
        self.assertEqual(ral_lcg2['service'], 'gLite-APEL')
        self.assertEqual(ral_lcg2['hostname'], 'arc-ce01.gridpp.rl.ac.uk')
        ral_lcg2_tags = ral_lcg2['tags']
        self.assertEqual(ral_lcg2_tags['scope'], 'EGI, wlcg, tier1, alice, atlas, cms, lhcb')
        self.assertEqual(ral_lcg2_tags['monitored'], '1')
        self.assertEqual(ral_lcg2_tags['production'], '1')

        # ASSERTIONS for AZ_IFAN
        az_ifan = ParseServiceEndpointsTest.get_group(self.group_endpoints, 'AZ-IFAN')
        self.assertIsNotNone(az_ifan)
        self.assertEqual(az_ifan['service'], 'CREAM-CE')
        self.assertEqual(az_ifan['hostname'], 'ce.physics.science.az')
        az_ifan_tags = az_ifan['tags']
        self.assertEqual(az_ifan_tags['scope'], 'EGI, wlcg, atlas')
        self.assertEqual(az_ifan_tags['monitored'], '1')
        self.assertEqual(az_ifan_tags['production'], '1')

        # Assert pass_extensions=False is working
        self.assertFalse(ParseServiceEndpointsTest.endpoints_have_extension(self.group_endpoints))

        parse_service_endpoints = ParseServiceEndpoints(logger, self.content, 'CUSTOMERFOO', uid=True, pass_extensions=True)
        # Assert customer name is set properly
        self.assertEqual(parse_service_endpoints.custname, CUSTOMER_NAME)
        group_endpoints = parse_service_endpoints.get_group_endpoints()
        # Assert pass_extensions=True is working 
        self.assertTrue(ParseServiceEndpointsTest.endpoints_have_extension(group_endpoints))

        # Assert uid=True is working
        temp_group = ParseServiceEndpointsTest.get_group(group_endpoints, 'GSI-LCG2')
        self.assertIsNotNone(temp_group)
        self.assertEqual(temp_group['hostname'], 'grid13.gsi.de_14G0')

        temp_group = ParseServiceEndpointsTest.get_group(group_endpoints, 'RAL-LCG2')
        self.assertIsNotNone(temp_group)
        self.assertEqual(temp_group['hostname'], 'arc-ce01.gridpp.rl.ac.uk_782G0')

        temp_group = ParseServiceEndpointsTest.get_group(group_endpoints, 'AZ-IFAN')
        self.assertIsNotNone(temp_group)
        self.assertEqual(temp_group['hostname'], 'ce.physics.science.az_1555G0')

        # Assert proper exception is thrown if empty xml is given to the function
        self.assertRaises(ConnectorError, ParseServiceEndpoints, logger, '', 'CUSTOMERFOO', uid=True, pass_extensions=True)

if __name__ == '__main__':
    unittest.main()