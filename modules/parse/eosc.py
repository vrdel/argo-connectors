from argo_egi_connectors.utils import module_class_name
from argo_egi_connectors.io.http import ConnectorHttpError

from urllib.parse import urlparse
import json


class ParseEoscTopo(object):
    def __init__(self, logger, data, uidservtype=False,
                 fetchtype='ServiceGroups', scope='EOSC'):
        self.data = data
        self.uidservtype = uidservtype
        self.fetchtype = fetchtype
        self.logger = logger
        self.scope = scope

    def _construct_fqdn(self, http_endpoint):
        return urlparse(http_endpoint).netloc

    def _parse_json(self, data):
        try:
            doc = json.loads(data)

        except ValueError as exc:
            self.logger.error('{} Customer:{} : Error parsing JSON feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc)))
            raise ConnectorHttpError()

        except Exception as exc:
            self.logger.error('{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(exc)))
            raise exc

        else:
            return doc

    def _get_groupgroups(self):
        groups = list()

        for entity in self._parse_json(self.data):
            tmp_dict = dict()

            tmp_dict['type'] = 'PROJECT'
            tmp_dict['group'] = 'EOSC'
            tmp_dict['subgroup'] = entity['SITENAME-SERVICEGROUP']
            tmp_dict['tags'] = {'monitored': '1', 'scope': self.scope}

            groups.append(tmp_dict)

        return groups

    def _get_groupendpoints(self):
        groups = list()

        for entity in self._parse_json(self.data):
            tmp_dict = dict()

            tmp_dict['type'] = self.fetchtype.upper()

            tmp_dict['service'] = entity['SERVICE_TYPE']
            tmp_dict['group'] = entity['SITENAME-SERVICEGROUP']
            info_url = entity['URL']
            if self.uidservtype:
                tmp_dict['hostname'] = '{1}_{0}'.format(entity['Service Unique ID'], self._construct_fqdn(info_url))
            else:
                tmp_dict['hostname'] = self._construct_fqdn(entity['URL'])
            tmp_dict['tags'] = {'scope': self.scope, 'monitored': '1', 'info_URL': info_url}

            groups.append(tmp_dict)

        return groups

    def get_data(self):
        return self._get_groupgroups(), self._get_groupendpoints()
