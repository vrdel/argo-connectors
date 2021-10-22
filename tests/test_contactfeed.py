import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts
from argo_egi_connectors.io.http import ConnectorError


logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


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

if __name__ == '__main__':
    unittest.main()
