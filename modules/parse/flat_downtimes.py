import datetime
import xml.dom.minidom

from xml.parsers.expat import ExpatError

from argo_connectors.exceptions import ConnectorParseError
from argo_connectors.parse.base import ParseHelpers
from argo_connectors.utils import construct_fqdn
from argo_connectors.utils import module_class_name


class ParseDowntimes(ParseHelpers):
    def __init__(self, logger, data, start, end, uid=False):
        self.data = self.csv_to_json(data)
        self.start = start
        self.end = end
        self.uid = uid

    def get_data(self):
        downtimes = list()

        for downtime in self.data:
            entry = dict()

            service_id = downtime['unique_id']
            if not service_id:
                continue

            hostname = construct_fqdn(downtime['url'])
            service_type = downtime['service_type']
            start_time = datetime.datetime.strptime(downtime['start_time'], "%m/%d/%Y %H:%M")
            end_time = datetime.datetime.strptime(downtime['end_time'], "%m/%d/%Y %H:%M")

            if self.uid:
                entry['hostname'] = '{0}_{1}'.format(hostname, service_id)
            else:
                entry['hostname'] = hostname

            downtime['service'] = service_type
            downtime['start_time'] = start_time.strftime('%Y-%m-%dT%H:%M:00Z')
            downtime['end_time'] = end_time.strftime('%Y-%m-%dT%H:%M:00Z')
            downtimes.append(downtime)

        return downtimes
