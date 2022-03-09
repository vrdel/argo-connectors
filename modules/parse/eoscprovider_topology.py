from urllib.parse import urlparse
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.base import ParseHelpers


class ParseResources(ParseHelpers):
    pass


class ParseProviders(ParseHelpers):
    def __init__(self, logger, data, custname):
        super().__init__(logger)
        self.logger = logger
        self.data = data
        self.custname = custname
        self._providers = dict()
        self._parse_data()

    def _parse_data(self):
        json_data = self.parse_json(self.data)
        for provider in json_data['results']:
            provider_name = provider['name']
            if provider_name not in self._providers:
                self._providers[provider_name] = {
                    'site': provider_name
                }
            scope = provider['tags']
            self._providers[provider_name]['scope'] = scope

    def get_group_groups(self):
        group_list, groupofgroups = list(), list()
        group_list = group_list + sorted([value for _, value in self._providers.items()], key=lambda s: s['ngi'])

        for group in group_list:
            tmpg = dict()
            tmpg['type'] = 'NGI'
            tmpg['group'] = group['ngi']
            tmpg['subgroup'] = group['site']
            tmpg['tags'] = {'scope': group.get('scope', '')}

            groupofgroups.append(tmpg)

        return groupofgroups


class ParseTopo(ParseProviders, ParseResources):
    def __init__(self, logger, resources, providers, custname):
        super(ParseTopo, self).__init__(logger, resources, custname)
        ParseResources.__init__(logger, providers, custname)
