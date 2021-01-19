import argparse
import datetime
import os
import sys


class GOCDBParse(object):
    def __init__(self, logger, data, start, end, uid=False):
        self.uid = uid
        self.start = start
        self.end = end
        self.data = data
        self.logger = logger

    def getText(nodelist):
        rc = []
        for node in nodelist:
            if node.nodeType == node.TEXT_NODE:
                rc.append(node.data)
        return ''.join(rc)

    def get_data(self):
        filteredDowntimes = list()

        downtimes = self.data.getElementsByTagName('DOWNTIME')
        try:
            for downtime in downtimes:
                classification = downtime.getAttributeNode('CLASSIFICATION').nodeValue
                hostname = self.getText(downtime.getElementsByTagName('HOSTNAME')[0].childNodes)
                serviceType = self.getText(downtime.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                startStr = self.getText(downtime.getElementsByTagName('FORMATED_START_DATE')[0].childNodes)
                end_str = self.getText(downtime.getElementsByTagName('FORMATED_END_DATE')[0].childNodes)
                severity = self.getText(downtime.getElementsByTagName('SEVERITY')[0].childNodes)
                try:
                    serviceId = self.getText(downtime.getElementsByTagName('PRIMARY_KEY')[0].childNodes)
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
            return filteredDowntimes

