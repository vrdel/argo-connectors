import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.mesh.contacts import attach_contacts_topodata

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

    def test_ConnectorParseErrorException(self):
        # Assert proper exception is thrown if empty xml is given to the function
        self.assertRaises(ConnectorParseError, ParseServiceEndpoints, logger, '', 'CUSTOMERFOO', uid=True, pass_extensions=True)


class MeshSitesAndContacts(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.maxDiff = None
        self.sample_sites_data = [
            {
                'group': 'iris.ac.uk',
                'subgroup': 'dirac-durham',
                'tags': {
                    'certification': 'Certified', 'infrastructure':
                    'Production', 'scope': 'iris.ac.uk'
                },
                'type': 'NGI'
            },
            {
                'group': 'Russia',
                'subgroup': 'RU-SARFTI',
                'tags': {
                    'certification': 'Certified', 'infrastructure':
                    'Production', 'scope': 'EGI'
                },
                'type': 'NGI'
            },
        ]
        self.sample_sites_contacts = [
            {
                'contacts': [
                                {
                                    'certdn': 'certdn1-dirac-durham',
                                    'email': 'name1.surname1@durham.ac.uk',
                                    'forename': 'Name1',
                                    'role': 'Site Administrator',
                                    'surname': 'Surname1'
                                },
                                {
                                    'certdn': 'certdn2-dirac-durham',
                                    'email': 'name2.surname2@durham.ac.uk',
                                    'forename': 'Name2',
                                    'role': 'Site Operations Manager',
                                    'surname': 'Surname2'
                                }
                ],
                'name': 'dirac-durham'
            },
            {
                'contacts': [
                                {
                                    'certdn': 'certdn1-ru-sarfti',
                                    'email': 'name1.surname1@gmail.com',
                                    'forename': 'Name1',
                                    'role': 'Site Administrator',
                                    'surname': 'Surname1'
                                },
                                {
                                    'certdn': 'certdn2-ru-sarfti',
                                    'email': 'name2.surname2@gmail.com',
                                    'forename': 'Name2',
                                    'role': 'Site Administrator',
                                    'surname': 'Surname2'
                                },
                ],
                'name': 'RU-SARFTI'
            },

        ]

    def test_SitesAndContacts(self):
        attach_contacts_topodata(logger, self.sample_sites_contacts, self.sample_sites_data)
        self.assertEqual(self.sample_sites_data[0],
            {
                'group': 'iris.ac.uk',
                'notifications': {'contacts': ['name1.surname1@durham.ac.uk',
                                               'name2.surname2@durham.ac.uk'],
                                  'enabled': True},
                'subgroup': 'dirac-durham',
                'tags': {'certification': 'Certified', 'infrastructure':
                         'Production', 'scope': 'iris.ac.uk'},
                'type': 'NGI'
            }
        )
        self.assertEqual(self.sample_sites_data[1],
            {
                'group': 'Russia',
                'notifications': {'contacts': ['name1.surname1@gmail.com',
                                               'name2.surname2@gmail.com'],
                                  'enabled': True},
                'subgroup': 'RU-SARFTI',
                'tags': {'certification': 'Certified', 'infrastructure':
                         'Production', 'scope': 'EGI'},
                'type': 'NGI'
            }
        )


class MeshServiceGroupsAndContacts(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.maxDiff = None
        self.sample_servicegroups_data = [
            {
                'group': 'EGI',
                'subgroup': 'NGI_ARMGRID_SERVICES',
                'tags': {
                    'monitored': '1',
                    'scope': 'EGI'
                },
                'type': 'PROJECT'
            },
            {
                'group': 'EGI',
                'subgroup': 'NGI_CYGRID_SERVICES',
                'tags': {
                    'monitored': '1',
                    'scope': 'EGI'
                },
                'type': 'PROJECT'
            },
        ]
        self.sample_servicegroup_contacts = [
            {
                'contacts': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
                'name': 'NGI_ARMGRID_SERVICES'
            },
            {
                'contacts': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com'],
                'name': 'NGI_CYGRID_SERVICES'
            },

        ]

    def test_ServiceGroupsAndContacts(self):
        attach_contacts_topodata(logger, self.sample_servicegroup_contacts,
                                 self.sample_servicegroups_data)
        self.assertEqual(self.sample_servicegroups_data[0],
            {
                'group': 'EGI',
                'subgroup': 'NGI_ARMGRID_SERVICES',
                'notifications': {
                    'contacts': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
                    'enabled': True
                },
                'tags': {
                    'monitored': '1',
                    'scope': 'EGI'
                },
                'type': 'PROJECT'
            }
        )


class MeshServiceEndpointsAndContacts(unittest.TestCase):
    def setUp(self):
        logger.customer = CUSTOMER_NAME
        self.maxDiff = None
        self.sample_serviceendpoints_data = [
            {
                'group': 'GROUP1',
                'hostname': 'fqdn1.com',
                'service': 'service1',
                'tags': {
                    'monitored': '1',
                    'production': '0',
                    'scope': ''
                },
                'type': 'SERVICEGROUPS'
            },
            {
                'group': 'GROUP2',
                'hostname': 'fqdn2.com',
                'service': 'service2',
                'tags': {
                    'monitored': '1',
                    'production': '0',
                    'scope': ''
                },
                'type': 'SERVICEGROUPS'
            }
        ]
        self.sample_serviceendpoints_contacts = [
            {
                'contacts': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
                'name': 'fqdn1.com+service1'
            },
            {
                'contacts': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com'],
                'name': 'fqdn2.com+service2'
            }
        ]

    def test_ServiceEndpointsAndContacts(self):
        attach_contacts_topodata(logger, self.sample_serviceendpoints_contacts,
                                 self.sample_serviceendpoints_data)
        self.assertEqual(self.sample_serviceendpoints_data[0],
            {
                'group': 'GROUP1',
                'hostname': 'fqdn1.com',
                'service': 'service1',
                'notifications': {
                    'contacts': ['Name1.Surname1@email.com', 'Name2.Surname2@email.com'],
                    'enabled': True
                },
                'tags': {
                    'monitored': '1',
                    'production': '0',
                    'scope': ''
                },
                'type': 'SERVICEGROUPS'
            }
        )
        self.assertEqual(self.sample_serviceendpoints_data[1],
            {
                'group': 'GROUP2',
                'hostname': 'fqdn2.com',
                'service': 'service2',
                'notifications': {
                    'contacts': ['Name3.Surname3@email.com', 'Name4.Surname4@email.com'],
                    'enabled': True
                },
                'tags': {
                    'monitored': '1',
                    'production': '0',
                    'scope': ''
                },
                'type': 'SERVICEGROUPS'
            }
        )


if __name__ == '__main__':
    unittest.main()
