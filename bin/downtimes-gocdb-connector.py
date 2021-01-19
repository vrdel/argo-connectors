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
from argo_egi_connectors.parse.gocdb_downtimes import GOCDBParse

logger = None

globopts = {}

DOWNTIMEPI = '/gocdbpi/private/?method=get_downtime'


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
                    % (logger.customer, 'authentication', ''.join(missing)))
        raise SystemExit(1)

    # we don't have multiple tenant definitions in one
    # customer file so we can safely assume one tenant/customer
    write_empty = confcust.send_empty(sys.argv[0])

    feed_parts = urlparse(topofeed)
    try:
        res = input.connection(logger, os.path.basename(sys.argv[0]), globopts, feed_parts.scheme, feed_parts.netloc,
                               DOWNTIMEPI + '&windowstart=%s&windowend=%s' % (start.strftime("%Y-%m-%d"),
                                                                                end.strftime("%Y-%m-%d")),
                               custauth=auth_opts)
        if not res:
            raise input.ConnectorError()

        gocdb = GOCDBParse(logger, res, start, end, uidservtype)
        if not write_empty:
            dts = gocdb.get_data()
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

        if eval(globopts['GeneralPublishWebAPI'.lower()]):
            webapi = output.WebAPI(sys.argv[0], webapi_opts['webapihost'],
                                   webapi_opts['webapitoken'], logger,
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

    except input.ConnectorError:
        self.state = False
        return []


if __name__ == '__main__':
    main()
