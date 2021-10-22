import xml.dom.minidom
from xml.parsers.expat import ExpatError

from argo_egi_connectors.io.http import ConnectorError
from argo_egi_connectors.parse.base import ParseHelpers
from argo_egi_connectors.utils import filename_date, module_class_name


class ParseRocContacts(ParseHelpers):
    def __init__(self, logger, data, custname):
        super().__init__(logger)
        self.data = data
        self.logger = logger
        self.data = data
        self.custname = custname
        self._parse_data()

    def _parse_data(self):
        return self._parse_contacts(self.data, 'ROC', 'CONTACT', 'ROCNAME')

    def get_contacts(self):
        return self._parse_data()


class ParseSiteContacts(ParseHelpers):
    def __init__(self, logger, data, custname):
        super().__init__(logger)
        self.logger = logger
        self.data = data
        self.custname = custname
        self._parse_data()

    def _parse_data(self):
        return self._parse_contacts(self.data, 'SITE', 'CONTACT', 'SHORT_NAME')

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
