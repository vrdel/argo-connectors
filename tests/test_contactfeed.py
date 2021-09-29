import unittest

from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts

logger = Logger('test_contactfeed.py')
CUSTOMER_NAME = 'CUSTOMERFOO'


class ParseSitesContactTest(unittest.TestCase):
    def setUp(self):
        with open('tests/sample-site_contacts.xml') as feed_file:
            self.content = feed_file.read()
        logger.customer = CUSTOMER_NAME
        parse_sites_contacts = ParseSiteContacts(logger, self.content, CUSTOMER_NAME)
        self.contacts = parse_sites_contacts.get_contacts()

    def test_LenContacts(self):
        len(self.contacts)


if __name__ == '__main__':
    unittest.main()
