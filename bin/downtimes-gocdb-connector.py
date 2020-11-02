#!/usr/bin/python3

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
from urllib.parse import urlparse

from argo_egi_connectors import input
from argo_egi_connectors import output
from argo_egi_connectors.log import Logger

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.helpers import filename_date, module_class_name

logger = None

DOWNTIMEPI = '/gocdbpi/private/?method=get_downtime'

globopts = {}


def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)


def all_same(elemlist):
    prev = None

    for el in elemlist:
        if prev is None:
            prev = el
        else:
            if el != prev:
                return False
            else:
                return True


class GOCDBReader(object):
    def __init__(self, feed, auth=None, uid=False):
        self._o = urlparse(feed)
        self.argDateFormat = "%Y-%m-%d"
        self.WSDateFormat = "%Y-%m-%d %H:%M"
        self.state = True
        self.custauth = auth
        self.uid = uid

    def getDowntimes(self, start, end):
        filteredDowntimes = list()

        try:
            res = input.connection(logger, module_class_name(self), globopts, self._o.scheme, self._o.netloc,
                                   DOWNTIMEPI + '&windowstart=%s&windowend=%s' % (start.strftime(self.argDateFormat),
                                                                                  end.strftime(self.argDateFormat)),
                                   custauth=self.custauth)
            if not res:
                raise input.ConnectorError()

            doc = input.parse_xml(logger, module_class_name(self), globopts,
                                  res, self._o.scheme + '://' + self._o.netloc + DOWNTIMEPI)

            if not doc:
                raise input.ConnectorError()

        except input.ConnectorError:
            self.state = False
            return []

        else:
            downtimes = doc.getElementsByTagName('DOWNTIME')
            try:
                for downtime in downtimes:
                    classification = downtime.getAttributeNode('CLASSIFICATION').nodeValue
                    hostname = getText(downtime.getElementsByTagName('HOSTNAME')[0].childNodes)
                    serviceType = getText(downtime.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                    startStr = getText(downtime.getElementsByTagName('FORMATED_START_DATE')[0].childNodes)
                    endStr = getText(downtime.getElementsByTagName('FORMATED_END_DATE')[0].childNodes)
                    severity = getText(downtime.getElementsByTagName('SEVERITY')[0].childNodes)
                    try:
                        serviceId = getText(downtime.getElementsByTagName('PRIMARY_KEY')[0].childNodes)
                    except IndexError:
                        serviceId = downtime.getAttribute('PRIMARY_KEY')
                    startTime = datetime.datetime.strptime(startStr, self.WSDateFormat)
                    endTime = datetime.datetime.strptime(endStr, self.WSDateFormat)

                    if (startTime < start):
                        startTime = start
                    if (endTime > end):
                        endTime = end

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
                self.state = False
                logger.error(module_class_name(self) + 'Customer:%s Job:%s : Error parsing feed %s - %s' % (logger.customer, logger.job,
                                                                                                            self._o.scheme + '://' + self._o.netloc + DOWNTIMEPI,
                                                                                                            repr(e).replace('\'', '')))
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
    topofeed = confcust.get_topofeed()
    logger.customer = confcust.get_custname()

    if len(args.date) == 0:
        print(parser.print_help())
        raise SystemExit(1)

    # calculate start and end times
    try:
        start = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        end = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=23, minute=59, second=59)
    except ValueError as e:
        logger.error(e)
        raise SystemExit(1)

    uidservtype = confcust.get_uidserviceendpoints()

    auth_custopts = confcust.get_authopts()
    auth_opts = cglob.merge_opts(auth_custopts, 'authentication')
    auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
    if not auth_complete:
        logger.error('Customer:%s %s options incomplete, missing %s'
                    % (logger.customer, 'authentication',
                        ''.join(missing)))
        raise SystemExit(1)

    # we don't have multiple tenant definitions in one
    # customer file so we can safely assume one tenant/customer
    write_empty = confcust.send_empty(sys.argv[0])

    gocdb = GOCDBReader(topofeed, auth_opts, uidservtype)
    if not write_empty:
        dts = gocdb.getDowntimes(start, end)
    else:
        dts = []
        gocdb.state = True

    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)

    # safely assume here one customer defined in customer file
    cust = list(confcust.get_customers())[0]
    statedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    output.write_state(sys.argv[0], statedir, gocdb.state, globopts['InputStateDays'.lower()], timestamp)

    if not gocdb.state:
        raise SystemExit(1)

    if eval(globopts['GeneralPublishWebAPI'.lower()]):
        webapi = output.WebAPI(sys.argv[0],
                               webapi_opts['webapihost'],
                               webapi_opts['webapitoken'],
                               logger,
                               int(globopts['ConnectionRetry'.lower()]),
                               int(globopts['ConnectionTimeout'.lower()]),
                               int(globopts['ConnectionSleepRetry'.lower()]),
                               date=args.date[0],
                               verifycert=globopts['AuthenticationVerifyServerCert'.lower()])
        webapi.send(dts, downtimes_component=True)

    custdir = confcust.get_custdir()
    if eval(globopts['GeneralWriteAvro'.lower()]):
        filename = filename_date(logger, globopts['OutputDowntimes'.lower()], custdir, stamp=timestamp)
        avro = output.AvroWriter(globopts['AvroSchemasDowntimes'.lower()], filename)
        ret, excep = avro.write(dts)
        if not ret:
            logger.error('Customer:%s %s' % (logger.customer, repr(excep)))
            raise SystemExit(1)

    if gocdb.state:
        logger.info('Customer:%s Fetched Date:%s Endpoints:%d' % (confcust.get_custname(cust),
                                                                  args.date[0], len(dts)))


if __name__ == '__main__':
    main()
