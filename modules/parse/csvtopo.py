from urllib.parse import urlparse
from argo_egi_connectors.tools import filename_date, module_class_name


class ParseServiceGroupsEndpoints(object):
    def __init__(self, logger, data, project, uidservtype=False,
                 fetchtype='ServiceGroups'):
        self.data = data
        self.uidservtype = uidservtype
        self.fetchtype = fetchtype
        self.logger = logger
        self.project = project

    def _construct_fqdn(self, http_endpoint):
        return urlparse(http_endpoint).netloc

    def get_groupgroups(self):
        try:
            groups = list()

            for entity in self.data:
                tmp_dict = dict()

                tmp_dict['type'] = 'PROJECT'
                tmp_dict['group'] = self.project
                tmp_dict['subgroup'] = entity['SITENAME-SERVICEGROUP']
                tmp_dict['tags'] = {'monitored': '1', 'scope': self.project}

                groups.append(tmp_dict)

            return groups

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing CSV feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc

    def get_groupendpoints(self):
        try:
            groups = list()

            for entity in self.data:
                tmp_dict = dict()

                tmp_dict['type'] = self.fetchtype.upper()
                tmp_dict['group'] = entity['SITENAME-SERVICEGROUP']
                tmp_dict['service'] = entity['SERVICE_TYPE']
                info_url = entity['URL']
                if self.uidservtype:
                    tmp_dict['hostname'] = '{1}_{0}'.format(entity['Service Unique ID'], self._construct_fqdn(info_url))
                else:
                    tmp_dict['hostname'] = self._construct_fqdn(entity['URL'])
                tmp_dict['tags'] = {'scope': self.project, 'monitored': '1', 'info.URL': info_url}

                groups.append(tmp_dict)

            return groups

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing CSV feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise exc
