import datetime
import xml.dom.minidom

from xml.parsers.expat import ExpatError
from argo_egi_connectors.utils import module_class_name
from argo_egi_connectors.parse.base import ConnectorParseError


class ParseDowntimes(object):
    def __init__(self, logger, data, start, end, uid=False):
        self.uid = uid
        self.start = start
        self.end = end
        self.logger = logger
        self.data = self._parse_xml(data)

    def _get_text(self, nodelist):
        node_collect = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                node_collect.append(node.data)
        return ''.join(node_collect)

    def _parse_xml(self, data):
        try:
            return xml.dom.minidom.parseString(data)

        except ExpatError as exc:
            msg = '{} Customer:{} : Error parsing XML feed - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            self.logger.error(msg)
            raise ConnectorParseError()

        except Exception as exc:
            msg = '{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(exc))
            self.logger.error(msg)
            raise exc

    def get_data(self):
        filtered_downtimes = list()

        downtimes = self.data.getElementsByTagName('DOWNTIME')
        try:
            for downtime in downtimes:
                classification = downtime.getAttributeNode('CLASSIFICATION').nodeValue
                hostname = self._get_text(downtime.getElementsByTagName('HOSTNAME')[0].childNodes)
                service_type = self._get_text(downtime.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                start_str = self._get_text(downtime.getElementsByTagName('FORMATED_START_DATE')[0].childNodes)
                end_str = self._get_text(downtime.getElementsByTagName('FORMATED_END_DATE')[0].childNodes)
                severity = self._get_text(downtime.getElementsByTagName('SEVERITY')[0].childNodes)
                try:
                    service_id = self._get_text(downtime.getElementsByTagName('PRIMARY_KEY')[0].childNodes)
                except IndexError:
                    service_id = downtime.getAttribute('PRIMARY_KEY')
                start_time = datetime.datetime.strptime(start_str, "%Y-%m-%d %H:%M")
                end_time = datetime.datetime.strptime(end_str, "%Y-%m-%d %H:%M")

                if start_time < self.start:
                    start_time = self.start
                if end_time > self.end:
                    end_time = self.end

                if classification == 'SCHEDULED' and severity == 'OUTAGE':
                    downtime = dict()
                    if self.uid:
                        downtime['hostname'] = '{0}_{1}'.format(hostname, service_id)
                    else:
                        downtime['hostname'] = hostname
                    downtime['service'] = service_type
                    downtime['start_time'] = start_time.strftime('%Y-%m-%dT%H:%M:00Z')
                    downtime['end_time'] = end_time.strftime('%Y-%m-%dT%H:%M:00Z')
                    filtered_downtimes.append(downtime)

            return filtered_downtimes

        except (KeyError, IndexError, AttributeError, TypeError, AssertionError) as exc:
            self.logger.error(module_class_name(self) + 'Customer:%s : Error parsing feed - %s' % (self.logger.customer, repr(exc).replace('\'', '')))
            return []
