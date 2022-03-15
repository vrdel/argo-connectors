from urllib.parse import urlparse
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.base import ParseHelpers


class ParseResources(ParseHelpers):
    def __init__(self, logger, data=None, custname=None):
        super(ParseResources, self).__init__(logger)
        self.data = data
        self.custname = custname
        self._resources = list()
        self._parse_data()

    def _parse_data(self):
        if type(self.data) == str:
            json_data = self.parse_json(self.data)
        else:
            json_data = self.data
        for resource in json_data['results']:
            self._resources.append({
                'id': resource['id'],
                'name': resource['name'],
                'provider': resource['resourceOrganisation'],
                'webpage': resource['webpage'],
                'scope': resource['tags'],
                'description': resource['description']
            })
        self.data = self._resources


class ParseProviders(ParseHelpers):
    def __init__(self, logger, data, custname):
        super(ParseProviders, self).__init__(logger)
        self.data = data
        self.custname = custname
        self._providers = list()
        self._parse_data()

    def _parse_data(self):
        if type(self.data) == str:
            json_data = self.parse_json(self.data)
        else:
            json_data = self.data
        for provider in json_data['results']:
            self._providers.append({
                'id': provider['id'],
                'website': provider['website'],
                'name': provider['name'],
                'abbr': provider['abbreviation'],
                'scope': provider['tags']
            })
        self.data = self._providers


class ParseTopo(object):
    def __init__(self, logger, providers, resources, custname):
        self.providers = ParseProviders(logger, providers, custname)
        self.resources = ParseResources(logger, resources, custname)

    def get_group_groups(self):
        gg = list()
        for provider in self.providers.data:
            resource_from_provider = list(filter(
                lambda resource: resource['provider'] == provider['id'],
                self.resources.data
            ))
            for resource in resource_from_provider:
                gge = dict()
                gge['type'] = 'PROJECT'
                gge['group'] = provider['abbr']
                gge['subgroup'] = resource['name']
                scopes = [scope.strip() for scope in provider['scope']]
                gge['tags'] = dict(scope=', '.join(scopes))
                gg.append(gge)

        return gg

    def get_group_endpoints(self):
        ge = list()
        for resource in self.resources.data:
            gee = dict()
            gee['type'] = 'SERVICGROUPS'
            gee['service'] = resource['id']
            gee['group'] = resource['name']
            gee['hostname'] = resource['webpage']
            scopes = [scope.strip() for scope in resource['scope']]
            gee['tags'] = dict(scope=', '.join(scopes))
            ge.append(gee)
        return ge
