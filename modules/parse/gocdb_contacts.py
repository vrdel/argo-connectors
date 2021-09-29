import xml.dom.minidom
from xml.parsers.expat import ExpatError

from argo_egi_connectors.io.http import ConnectorError


class ParseRocContacts(object):
    def __init__(self, logger, data, custname):
        super().__init__(logger)
        self.data = data

    def _parse_data(self):
        pass

    def contacts(self):
        pass


class ParseSiteContacts(object):
    def __init__(self, logger, data, custname):
        super().__init__(logger)
        self.data = data

    def _parse_data(self):
        pass

    def contacts(self):
        pass


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
