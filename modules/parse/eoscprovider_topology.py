from urllib.parse import urlparse
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.base import ParseHelpers


class ParseResources(ParseHelpers):
    def __init__(self, logger, data=None, custname=None):
        super(ParseResources, self).__init__(logger)
        self.data = data
        self.custname = custname
        self._resources = dict()
        self._parse_data()

    def _parse_data(self):
        json_data = self.parse_json(self.data)
        for resource in json_data['results']:
            resource_id = resource['id']
            if resource_id not in self._resources:
                self._resources[resource_id] = {
                    'name': resource['name'],
                    'provider': resource['resourceOrganisation'],
                    'webpage': resource['webpage'],
                    'scope': resource['tags'],
                    'description': resource['description']
                }

    def get_group_endpoints(self):
        return self._resources


class ParseProviders(ParseHelpers):
    def __init__(self, logger, data, custname):
        super(ParseProviders, self).__init__(logger)
        self.data = data
        self.custname = custname
        self._providers = dict()
        self._parse_data()

    def _parse_data(self):
        json_data = self.parse_json(self.data)
        for provider in json_data['results']:
            provider_id = provider['id']
            if provider_id not in self._providers:
                self._providers[provider_id] = {
                    'resources': list(),
                    'website': provider['website'],
                    'name': provider['name'],
                    'abbr': provider['abbreviation'],
                    'scope': provider['tags']
                }

    def get_group_groups(self):
        return self._providers


class ParseTopo(object):
    def __init__(self, logger, providers, resources, custname):
        self.providers = ParseProviders(logger, providers, custname)
        self.resources = ParseResources(logger, resources, custname)

    def get_group_groups(self):
        return self.providers.get_group_groups()

    def get_group_endpoints(self):
        return self.resources.get_group_endpoints()
