import xml.dom.minidom
from xml.parsers.expat import ExpatError

from argo_egi_connectors.io.http import ConnectorError
from argo_egi_connectors.parse.base import ParseHelpers
from argo_egi_connectors.utils import filename_date, module_class_name


class ParseRocContacts(ParseHelpers):
    def __init__(self, logger, data, custname):
        super().__init__(logger)
        self.data = data

    def _parse_data(self):
        pass

    def contacts(self):
        pass


class ParseSiteContacts(ParseHelpers):
    def __init__(self, logger, data, custname):
        self.logger = logger
        self.data = data
        self.custname = custname
        self._parse_data()

    def _parse_data(self):
        try:
            data = list()
            xml_data = self._parse_xml(self.data)
            sites = xml_data.getElementsByTagName('SITE')
            for site in sites:
                contacts = site.getElementsByTagName('CONTACT')
                for contact in contacts:
                    interested = ('EMAIL',)
                    email = self._parse_contact(contact, *interested)
                    data.append({
                        'email': email
                    })

            return data

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def get_contacts(self):
        return self._parse_data()


class ParseProjectContacts(object):
    def __init__(self, logger, data, custname):
        super().__init__(logger)
        self.data = data

    def _parse_data(self):
        pass

    def contacts(self):
        pass


class ParseServiceGroupRoles(object):
    def __init__(self, logger, data, custname):
        super().__init__(logger)
        self.data = data

    def _parse_data(self):
        pass

    def contacts(self):
        pass
