import argparse
import datetime
import os
import sys

from argo_egi_connectors.helpers import filename_date, module_class_name

import xml.dom.minidom
from xml.parsers.expat import ExpatError


class GOCDBParse(object):
    def __init__(self, logger, data, start, end, uid=False):
        self.uid = uid
        self.start = start
        self.end = end
        self.data = self._parse_xml(data)
        self.logger = logger

    def _get_text(nodelist):
        rc = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    def _parse_xml(self, data):
        try:
            return xml.dom.minidom.parseString(data)

        except ExpatError as e:
            if getattr(self.logger, 'job', False):
                msg = '{} Customer:{} Job:{} : Error parsing XML feed - {}'.format(module_class_name(self), self.logger.customer, self.logger.job, repr(e))
            else:
                msg = '{} Customer:{} : Error parsing XML feed - {}'.format(module_class_name(self), self.logger.customer, repr(e))
            self.logger.error(msg)
            raise ConnectorError()

        except Exception as e:
            if getattr(self.logger, 'job', False):
                msg = '{} Customer:{} Job:{} : Error - {}'.format(module_class_name(self), self.logger.customer, self.logger.job, repr(e))
            else:
                msg = '{} Customer:{} : Error - {}'.format(module_class_name(self), self.logger.customer, repr(e))
            self.logger.error(msg)
            raise e

    def get_data(self):
        filteredDowntimes = list()

        downtimes = self.data.getElementsByTagName('DOWNTIME')
        try:
            for downtime in downtimes:
                classification = downtime.getAttributeNode('CLASSIFICATION').nodeValue
                hostname = self._get_text(downtime.getElementsByTagName('HOSTNAME')[0].childNodes)
                serviceType = self._get_text(downtime.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                startStr = self._get_text(downtime.getElementsByTagName('FORMATED_START_DATE')[0].childNodes)
                end_str = self._get_text(downtime.getElementsByTagName('FORMATED_END_DATE')[0].childNodes)
                severity = self._get_text(downtime.getElementsByTagName('SEVERITY')[0].childNodes)
                try:
                    serviceId = self._get_text(downtime.getElementsByTagName('PRIMARY_KEY')[0].childNodes)
                except IndexError:
                    serviceId = downtime.getAttribute('PRIMARY_KEY')
                startTime = datetime.datetime.strptime(startStr, "%Y-%m-%d %H:%M")
                endTime = datetime.datetime.strptime(end_str, "%Y-%m-%d %H:%M")

                if (startTime < self.start):
                    startTime = self.start
                if (endTime > self.end):
                    endTime = self.end

                if classification == 'SCHEDULED' and severity == 'OUTAGE':
                    dt = dict()
                    if self.uid:
                        dt['hostname'] = '{0}_{1}'.format(hostname, serviceId)
                    else:
                        dt['hostname'] = hostname
                    dt['service'] = serviceType
                    dt['start_time'] = startTime.strftime('%Y-%m-%d %H:%M').replace(' ', 'T', 1).replace(' ', ':') + ':00Z'
                    dt['end_time'] = endTime.strftime('%Y-%m-%d %H:%M').replace(' ', 'T', 1).replace(' ', ':') + ':00Z'
                    filteredDowntimes.append(dt)

        except (KeyError, IndexError, AttributeError, TypeError, AssertionError) as e:
            self.logger.error(module_class_name(self) + 'Customer:%s Job:%s : Error parsing feed %s - %s' % (self.logger.customer, self.logger.job,
                                                                                                        repr(e).replace('\'', '')))
            return []
        else:
            return self._parse_xml(filteredDowntimes)
