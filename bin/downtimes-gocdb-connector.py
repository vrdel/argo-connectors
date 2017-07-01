#!/usr/bin/python

# Copyright (c) 2013 GRNET S.A., SRCE, IN2P3 CNRS Computing Centre
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the
# License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an "AS
# IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language
# governing permissions and limitations under the License.
#
# The views and conclusions contained in the software and
# documentation are those of the authors and should not be
# interpreted as representing official policies, either expressed
# or implied, of either GRNET S.A., SRCE or IN2P3 CNRS Computing
# Centre
#
# The work represented by this source file is partially funded by
# the EGI-InSPIRE project through the European Commission's 7th
# Framework Programme (contract # INFSO-RI-261323)

import argparse
import datetime
import os
import sys
from urlparse import urlparse

from argo_egi_connectors import input
from argo_egi_connectors import output
from argo_egi_connectors.log import SingletonLogger as Logger

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.helpers import filename_date, module_class_name

logger = Logger(os.path.basename(sys.argv[0]))

DOWNTIMEPI = '/gocdbpi/private/?method=get_downtime'

globopts = {}

class GOCDBReader(object):
    def __init__(self, feed):
        self._o = urlparse(feed)
        self.argDateFormat = "%Y-%m-%d"
        self.WSDateFormat = "%Y-%m-%d %H:%M"
        self.state = True

    def getDowntimes(self, start, end):
        filteredDowntimes = list()

        try:
            res = input.connection(logger, module_class_name(self), globopts, self._o.scheme, self._o.netloc,
                                   DOWNTIMEPI + '&windowstart=%s&windowend=%s' % (start.strftime(self.argDateFormat),
                                                                                  end.strftime(self.argDateFormat)))

            doc = input.parse_xml(logger, module_class_name(self), globopts,
                                  res, self._o.scheme + '://' + self._o.netloc + DOWNTIMEPI)

        except input.ConnectorError:
            self.state = False
            return []

        else:
            downtimes = doc.getElementsByTagName('DOWNTIME')
            try:
                for downtime in downtimes:
                    classification = downtime.getAttributeNode('CLASSIFICATION').nodeValue
                    hostname = downtime.getElementsByTagName('HOSTNAME')[0].childNodes[0].data
                    serviceType = downtime.getElementsByTagName('SERVICE_TYPE')[0].childNodes[0].data
                    startStr = downtime.getElementsByTagName('FORMATED_START_DATE')[0].childNodes[0].data
                    assert startStr != ''
                    endStr = downtime.getElementsByTagName('FORMATED_END_DATE')[0].childNodes[0].data
                    assert endStr != ''
                    severity = downtime.getElementsByTagName('SEVERITY')[0].childNodes[0].data

                    startTime = datetime.datetime.strptime(startStr, self.WSDateFormat)
                    endTime = datetime.datetime.strptime(endStr, self.WSDateFormat)

                    if (startTime < start):
                        startTime = start
                    if (endTime > end):
                        endTime = end

                    if classification == 'SCHEDULED' and severity == 'OUTAGE':
                        dt = dict()
                        dt['hostname'] = hostname
                        dt['service'] = serviceType
                        dt['start_time'] = startTime.strftime('%Y-%m-%d %H:%M').replace(' ', 'T', 1).replace(' ', ':') + ':00Z'
                        dt['end_time'] = endTime.strftime('%Y-%m-%d %H:%M').replace(' ', 'T', 1).replace(' ', ':') + ':00Z'
                        filteredDowntimes.append(dt)

            except (KeyError, IndexError, AttributeError, TypeError, AssertionError) as e:
                self.state = False
                logger.error(module_class_name(self) + ': Error parsing feed %s - %s' % (self._o.scheme + '://' + self._o.netloc + DOWNTIMEPI,
                                                                                         repr(e).replace('\'','')))
                return []
            else:
                return filteredDowntimes

def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description='Fetch downtimes from GOCDB for given date')
    parser.add_argument('-d', dest='date', nargs=1, metavar='YEAR-MONTH-DAY', required=True)
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()

    logger = Logger(os.path.basename(sys.argv[0]))
    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    feeds = confcust.get_mapfeedjobs(sys.argv[0], deffeed='https://goc.egi.eu/gocdbpi/')


    if len(args.date) == 0:
        print parser.print_help()
        raise SystemExit(1)

    # calculate start and end times
    try:
        start = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        end = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
    except ValueError as e:
        logger.error(e)
        raise SystemExit(1)

    start = start.replace(hour=0, minute=0, second=0)
    end = end.replace(hour=23, minute=59, second=59)

    for feed, jobcust in feeds.items():
        gocdb = GOCDBReader(feed)
        dts = gocdb.getDowntimes(start, end)

        for job, cust in jobcust:
            jobdir = confcust.get_fulldir(cust, job)
            jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)

            custname = confcust.get_custname(cust)
            ams_custopts = confcust.get_amsopts(cust)
            ams_opts = cglob.merge_opts(ams_custopts, 'ams')
            ams_complete, missopt = cglob.is_complete(ams_opts, 'ams')
            if not ams_complete:
                logger.error('Customer:%s %s options incomplete, missing %s' % (custname, 'ams', ' '.join(missopt)))
                continue

            output.write_state(sys.argv[0], jobstatedir, gocdb.state, globopts['InputStateDays'.lower()], timestamp)

            if not gocdb.state:
                continue

            if eval(globopts['GeneralPublishAms'.lower()]):
                ams = output.AmsPublish(ams_opts['amshost'],
                                        ams_opts['amsproject'],
                                        ams_opts['amstoken'],
                                        ams_opts['amstopic'],
                                        confcust.get_jobdir(job),
                                        ams_opts['amsbulk'],
                                        logger,
                                        int(globopts['ConnectionRetry'.lower()]),
                                        int(globopts['ConnectionTimeout'.lower()]))

                ams.send(globopts['AvroSchemasDowntimes'.lower()], 'downtimes',
                         timestamp.replace('_', '-'), dts)

            if eval(globopts['GeneralWriteAvro'.lower()]):
                filename = filename_date(logger, globopts['OutputDowntimes'.lower()], jobdir, stamp=timestamp)
                avro = output.AvroWriter(globopts['AvroSchemasDowntimes'.lower()], filename)
                ret, excep = avro.write(dts)
                if not ret:
                    logger.error(excep)
                    raise SystemExit(1)

        if gocdb.state:
            custs = set([cust for job, cust in jobcust])
            for cust in custs:
                jobs = [job for job, lcust in jobcust if cust == lcust]
                logger.info('Customer:%s Jobs:%d Fetched Date:%s Endpoints:%d' % (cust, len(jobs), args.date[0], len(dts)))


if __name__ == '__main__':
    main()
