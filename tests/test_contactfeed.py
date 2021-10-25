import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts, ParseRocContacts
from argo_egi_connectors.parse.gocdb_topology import ParseServiceEndpoints, ConnectorError
from argo_egi_connectors.io.http import ConnectorError


logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseRocContactTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-roc_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_roc_contacts = ParseRocContacts(logger, self.content, CUSTOMER_NAME)
        self.roc_contacts = parse_roc_contacts.get_contacts()

    def test_formatContacts(self):
        self.assertEqual(self.roc_contacts[0],
            {
                'name': 'CERN',
                'contacts': [
                    {
                        'certdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=Name1/CN=11111/CN=Name1 Surname1',
                        'email': 'Name1.Surname1@example.com',
                        'forename': 'Name1',
                        'role': 'NGI Operations Manager',
                        'surname': 'Surname1'
                    },
                    {
                        'certdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=Name2/CN=111111/CN=Name2 Surname2',
                        'email': 'Name2.Surname2@example.com',
                        'forename': 'Name2',
                        'role': 'NGI Security Officer',
                        'surname': 'Surname2'
                    },
                    {
                        'certdn': '/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=Name3/CN=222222/CN=Name3 Surname3',
                        'email': 'Name3.Surname3@example.com',
                        'forename': 'Name3',
                        'role': 'NGI Operations Deputy Manager',
                        'surname': 'Surname3'
                    }
                ]
            }
        )


class ParseSitesContactTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-site_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_sites_contacts = ParseSiteContacts(logger, self.content, CUSTOMER_NAME)
        self.site_contacts = parse_sites_contacts.get_contacts()

    def test_lenContacts(self):
        self.assertEqual(len(self.site_contacts), 2)
        site_1 = len(self.site_contacts[0]['contacts'])
        site_2 = len(self.site_contacts[1]['contacts'])
        self.assertEqual(9, site_1 + site_2)

    def test_malformedContacts(self):
        self.assertRaises(ConnectorError, ParseSiteContacts, logger, 'wrong mocked data', CUSTOMER_NAME)

    def test_formatContacts(self):
        self.assertEqual(self.site_contacts[0],
            {
                'name': 'Site1',
                'contacts': [
                    {
                        'certdn': '/C=HR/O=CROGRID/O=SRCE/CN=Name1 Surname1',
                        'email': 'Name1.Surname1@email.hr',
                        'forename': 'Name1',
                        'role': 'Site Security Officer',
                        'surname': 'Surname1'
                    },
                    {
                        'certdn': '/C=HR/O=CROGRID/O=SRCE/CN=Name1 Surname1',
                        'email': 'Name1.Surname1@email.hr',
                        'forename': 'Name1',
                        'role': 'Site Operations Manager',
                        'surname': 'Surname1'
                    },
                    {
                        'certdn': '/C=HR/O=CROGRID/O=SRCE/CN=Name2 Surname2',
                        'email': 'Name2.Surname2@email.hr',
                        'forename': 'Name2',
                        'role': 'Site Operations Manager',
                        'surname': 'Surname2'
                    }
                ],
            }
        )


class ParseServiceEndpointsWithContactsTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-service_endpoint_with_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_service_endpoints = ParseServiceEndpoints(logger, self.content, CUSTOMER_NAME)
        self.group_endpoints = parse_service_endpoints.get_group_endpoints()

        parse_service_endpoints_ext = ParseServiceEndpoints(logger, self.content, 'CUSTOMERFOO', uid=True, pass_extensions=True)
        self.group_endpoints_ext = parse_service_endpoints_ext.get_group_endpoints()

    def test_LenEndpoints(self):
        self.assertEqual(len(self.group_endpoints),


if __name__ == '__main__':
    unittest.main()
