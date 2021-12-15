from urllib.parse import urlparse
from argo_egi_connectors.utils import filename_date, module_class_name
from argo_egi_connectors.exceptions import ConnectorParseError

import csv
import json
from io import StringIO


class ParseFlatEndpoints(object):
    def __init__(self, logger, data, project, uidservtype=False,
                 fetchtype='ServiceGroups', is_csv=False):
        if is_csv:
            self.data = self._csv_to_json(data)
        else:
            self.data = data
        self.uidservtype = uidservtype
        self.fetchtype = fetchtype
        self.logger = logger
        self.project = project

    def _construct_fqdn(self, http_endpoint):
        return urlparse(http_endpoint).netloc

    def _csv_to_json(self, csvdata):
        data = StringIO(csvdata)
        reader = csv.reader(data, delimiter=',')

        num_row = 0
        results = []
        header = []
        for row in reader:
            if num_row == 0:
                header = row
                num_row = num_row + 1
                continue
            num_item = 0
            datum = {}
            for item in header:
                datum[item] = row[num_item]
                num_item = num_item + 1
            results.append(datum)

        return results

    def get_groupgroups(self):
        try:
            groups = list()
            already_added = list()

            for entity in self.data:
                tmp_dict = dict()

                tmp_dict['type'] = 'PROJECT'
                tmp_dict['group'] = self.project
                tmp_dict['subgroup'] = entity['SITENAME-SERVICEGROUP']
                tmp_dict['tags'] = {'monitored': '1', 'scope': self.project}

                if tmp_dict['subgroup'] in already_added:
                    continue
                else:
                    groups.append(tmp_dict)
                already_added.append(tmp_dict['subgroup'])

            return groups

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing CSV feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError

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

                tmp_dict['tags'] = {'scope': self.project,
                                    'monitored': '1',
                                    'info_URL': info_url,
                                    'hostname': self._construct_fqdn(entity['URL'])}

                tmp_dict['tags'].update({'info_id': str(entity['Service Unique ID'])})
                groups.append(tmp_dict)

            return groups

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing CSV feed - %s' % (self.logger.customer, repr(exc).replace('\'', '').replace('\"', '')))
            raise ConnectorParseError
