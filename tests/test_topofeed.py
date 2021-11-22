import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites, ConnectorHttpError

logger = Logger('test_topofeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'

# Help function - check if any of endpoints contains extensions
# Used for checking if pass_extensions is working properly
def endpoints_have_extension(group_endpoints):
    for gren in group_endpoints:
        for key in gren['tags'].keys():
            if key.startswith('info_ext_'):
                return True
    return False

# Returns element of group_endpoints with given group_name or None
def get_group(group_endpoints, group_name):
    for group in group_endpoints:
        if group['group'] == group_name:
            return group

    return None


class ParseServiceEndpointsTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_endpoint.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_service_endpoints = ParseServiceEndpoints(logger, self.content, CUSTOMER_NAME)
        self.group_endpoints = parse_service_endpoints.get_group_endpoints()

        parse_service_endpoints_ext = ParseServiceEndpoints(logger, self.content, 'CUSTOMERFOO', uid=True, pass_extensions=True)
        self.group_endpoints_ext = parse_service_endpoints_ext.get_group_endpoints()

    def test_LenEndpoints(self):
        self.assertEqual(len(self.group_endpoints), 3) # Parsed correct number of endpoint groups

    def test_DataEndpoints(self):
        # Assertions for GSI_LCG2
        gsi_lcg2 = get_group(self.group_endpoints, 'GSI-LCG2')
        self.assertIsNotNone(gsi_lcg2)
        self.assertEqual(gsi_lcg2['service'], 'CE')
        self.assertEqual(gsi_lcg2['hostname'], 'grid13.gsi.de')
        gsi_lcg2_tags = gsi_lcg2['tags']
        self.assertEqual(gsi_lcg2_tags['scope'], 'EGI, wlcg, tier2, alice')
        self.assertEqual(gsi_lcg2_tags['monitored'], '0')
        self.assertEqual(gsi_lcg2_tags['production'], '0')

        # Assertions for RAL_LCG2
        ral_lcg2 = get_group(self.group_endpoints, 'RAL-LCG2')
        self.assertIsNotNone(ral_lcg2)
        self.assertEqual(ral_lcg2['service'], 'gLite-APEL')
        self.assertEqual(ral_lcg2['hostname'], 'arc-ce01.gridpp.rl.ac.uk')
        ral_lcg2_tags = ral_lcg2['tags']
        self.assertEqual(ral_lcg2_tags['scope'], 'EGI, wlcg, tier1, alice, atlas, cms, lhcb')
        self.assertEqual(ral_lcg2_tags['monitored'], '1')
        self.assertEqual(ral_lcg2_tags['production'], '1')

        # Assertions for AZ_IFAN
        az_ifan = get_group(self.group_endpoints, 'AZ-IFAN')
        self.assertIsNotNone(az_ifan)
        self.assertEqual(az_ifan['service'], 'CREAM-CE')
        self.assertEqual(az_ifan['hostname'], 'ce.physics.science.az')
        az_ifan_tags = az_ifan['tags']
        self.assertEqual(az_ifan_tags['scope'], 'EGI, wlcg, atlas')
        self.assertEqual(az_ifan_tags['monitored'], '1')
        self.assertEqual(az_ifan_tags['production'], '1')

    def test_HaveExtensions(self):
        # Assert pass_extensions=False is working
        self.assertFalse(endpoints_have_extension(self.group_endpoints))

    def test_EnabledExtensions(self):
        # Assert pass_extensions=True is working
        self.assertTrue(endpoints_have_extension(self.group_endpoints_ext))

    def test_SuffixUid(self):
        # Assert uid=True is working
        temp_group = get_group(self.group_endpoints_ext, 'GSI-LCG2')
        self.assertIsNotNone(temp_group)
        self.assertEqual(temp_group['hostname'], 'grid13.gsi.de_14G0')

        temp_group = get_group(self.group_endpoints_ext, 'RAL-LCG2')
        self.assertIsNotNone(temp_group)
        self.assertEqual(temp_group['hostname'], 'arc-ce01.gridpp.rl.ac.uk_782G0')

        temp_group = get_group(self.group_endpoints_ext, 'AZ-IFAN')
        self.assertIsNotNone(temp_group)
        self.assertEqual(temp_group['hostname'], 'ce.physics.science.az_1555G0')

    def test_ConnectorHttpErrorException(self):
        # Assert proper exception is thrown if empty xml is given to the function
        self.assertRaises(ConnectorHttpError, ParseServiceEndpoints, logger, '', 'CUSTOMERFOO', uid=True, pass_extensions=True)


if __name__ == '__main__':
    unittest.main()
