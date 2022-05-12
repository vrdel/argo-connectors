from urllib.parse import urlparse
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError
from argo_egi_connectors.parse.base import ParseHelpers


SERVICE_NAME_WEBPAGE='eu.eosc.portal.services.url'


def construct_fqdn(http_endpoint):
    return urlparse(http_endpoint).netloc


class ParseResources(ParseHelpers):
    def __init__(self, logger, data=None, custname=None):
        super(ParseResources, self).__init__(logger)
        self.data = data
        self.custname = custname
        self._resources = list()
        self._parse_data()

    def _parse_data(self):
        try:
            if type(self.data) == str:
                json_data = self.parse_json(self.data)
            else:
                json_data = self.data
            for resource in json_data['results']:
                self._resources.append({
                    'id': resource['id'],
                    'hardcoded_service': SERVICE_NAME_WEBPAGE,
                    'name': resource['name'],
                    'provider': resource['resourceOrganisation'],
                    'webpage': resource['webpage'],
                    'resource_tag': resource['tags'],
                    'description': resource['description']
                })
            self.data = self._resources

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing EOSC Resources feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc


class ParseProviders(ParseHelpers):
    def __init__(self, logger, data, custname):
        super(ParseProviders, self).__init__(logger)
        self.data = data
        self.custname = custname
        self._providers = list()
        self._parse_data()

    def _parse_data(self):
        try:
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
                    'provider_tag': provider['tags']
                })
            self.data = self._providers

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            msg = module_class_name(self) + ' Customer:%s : Error parsing EOSC Providers feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', ''))
            raise ConnectorParseError(msg)

        except ConnectorParseError as exc:
            raise exc


class ParseTopo(object):
    def __init__(self, logger, providers, resources, uidservendp, custname):
        self.uidservendp = uidservendp
        self.providers = ParseProviders(logger, providers, custname)
        self.resources = ParseResources(logger, resources, custname)
        self.maxDiff = None

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
                gge['subgroup'] = resource['id']
                if provider.get('provider_tag', False):
                    provider_tags = [tag.strip() for tag in provider['provider_tag']]
                    gge['tags'] = dict(provider_tags=', '.join(provider_tags))
                else:
                    gge['tags'] = dict()
                gg.append(gge)

        return gg

    def get_group_endpoints(self):
        ge = list()
        for resource in self.resources.data:
            gee = dict()
            gee['type'] = 'SERVICEGROUPS'
            gee['service'] = resource['hardcoded_service']
            gee['group'] = resource['id']
            if self.uidservendp:
                gee['hostname'] = '{}_{}'.format(construct_fqdn(resource['webpage']), resource['id'])
            else:
                gee['hostname'] = construct_fqdn(resource['webpage'])
            if resource.get('resource_tag', False):
                resource_tags = [tag.strip() for tag in resource['resource_tag']]
                gee['tags'] = dict(service_tags=', '.join(resource_tags),
                                   info_URL=resource['webpage'],
                                   info_ID=resource['id'],
                                   info_groupname=resource['name'])
            else:
                gee['tags'] = dict(info_URL=resource['webpage'],
                                   info_ID=resource['id'],
                                   info_groupname=resource['name'])
            if self.uidservendp:
                gee['tags'].update(dict(hostname=construct_fqdn(resource['webpage'])))
            ge.append(gee)

        return ge
