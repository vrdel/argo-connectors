import xml.dom.minidom
from xml.parsers.expat import ExpatError

from argo_egi_connectors.parse.base import ParseContacts
from argo_egi_connectors.parse.gocdb_topology import ParseServiceEndpoints
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError


class ParseRocContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.data = data
        self.logger = logger
        self.data = data
        self._parse_data()

    def _parse_data(self):
        try:
            return self.parse_contacts(self.data, 'ROC', 'CONTACT', 'ROCNAME')

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()


class ParseSiteContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.logger = logger
        self.data = data
        self._parse_data()

    def _parse_data(self):
        try:
            return self.parse_contacts(self.data, 'SITE', 'CONTACT', 'SHORT_NAME')

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()


class ParseSitesWithContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.logger = logger
        self.data = data
        self._parse_data()

    def _parse_data(self):
        try:
            return self.parse_sites_with_contacts(self.data)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()


class ParseServiceEndpointContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.logger = logger
        self.data = data

    def _parse_data(self):
        try:
            return self.parse_serviceendpoint_contacts(self.data)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError

    def get_contacts(self):
        return self._parse_data()


class ParseProjectContacts(object):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.data = data

    def _parse_data(self):
        pass

    def contacts(self):
        pass


class ParseServiceGroupWithContacts(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.data = data

    def _parse_data(self):
        try:
            return self.parse_servicegroups_with_contacts(self.data)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError


    def get_contacts(self):
        return self._parse_data()


class ParseServiceGroupRoles(ParseContacts):
    def __init__(self, logger, data):
        super().__init__(logger)
        self.data = data

    def _parse_data(self):
        try:
            return self.parse_servicegroup_contacts(self.data)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError, ExpatError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError


    def get_contacts(self):
        return self._parse_data()
