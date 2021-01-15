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
import os
import sys

from argo_egi_connectors import input
from argo_egi_connectors import output
from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.vapor import VaporParse

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.helpers import filename_date, module_class_name, date_check
from urllib.parse import urlparse

globopts = {}
logger = None

VAPORPI = 'https://operations-portal.egi.eu/vapor/downloadLavoisier/option/json/view/VAPOR_Ngi_Sites_Info'


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description="""Fetch weights information from Gstat provider
                                                    for every job listed in customer.conf""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    args = parser.parse_args()

    logger = Logger(os.path.basename(sys.argv[0]))

    fixed_date = None
    if args.date and date_check(args.date):
        fixed_date = args.date

    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    feeds = confcust.get_mapfeedjobs(sys.argv[0], deffeed=VAPORPI)

    for feed, jobcust in feeds.items():
        customers = set(map(lambda jc: confcust.get_custname(jc[1]), jobcust))
        customers = customers.pop() if len(customers) == 1 else '({0})'.format(','.join(customers))
        sjobs = set(map(lambda jc: jc[0], jobcust))
        jobs = list(sjobs)[0] if len(sjobs) == 1 else '({0})'.format(','.join(sjobs))
        logger.job = jobs
        logger.customer = customers

        try:
            feed_parts = urlparse(feed)
            res = input.connection(logger, os.path.basename(sys.argv[0]), globopts,
                                   feed_parts.scheme, feed_parts.netloc,
                                   feed_parts.path)
            if not res:
                raise input.ConnectorError()

            weights = VaporParse(logger, res).get_data()

            for job, cust in jobcust:
                logger.customer = confcust.get_custname(cust)
                logger.job = job

                write_empty = confcust.send_empty(sys.argv[0], cust)

                if write_empty:
                    weights = []

                jobdir = confcust.get_fulldir(cust, job)
                jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)

                webapi_custopts = confcust.get_webapiopts(cust)
                webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
                webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
                if not webapi_complete:
                    logger.error('Customer:%s Job:%s %s options incomplete, missing %s' % (logger.customer, job, 'webapi', ' '.join(missopt)))
                    continue

                if fixed_date:
                    output.write_state(sys.argv[0], jobstatedir, True,
                                       globopts['InputStateDays'.lower()],
                                       fixed_date.replace('-', '_'))
                else:
                    output.write_state(sys.argv[0], jobstatedir, True,
                                       globopts['InputStateDays'.lower()])

                if eval(globopts['GeneralPublishWebAPI'.lower()]):
                    webapi = output.WebAPI(sys.argv[0],
                                           webapi_opts['webapihost'],
                                           webapi_opts['webapitoken'], logger,
                                           int(globopts['ConnectionRetry'.lower()]),
                                           int(globopts['ConnectionTimeout'.lower()]),
                                           int(globopts['ConnectionSleepRetry'.lower()]),
                                           report=confcust.get_jobdir(job),
                                           endpoints_group='SITES',
                                           date=fixed_date,
                                           verifycert=globopts['AuthenticationVerifyServerCert'.lower()])
                    webapi.send(weights)

                if eval(globopts['GeneralWriteAvro'.lower()]):
                    if fixed_date:
                        filename = filename_date(logger, globopts['OutputWeights'.lower()], jobdir, fixed_date.replace('-', '_'))
                    else:
                        filename = filename_date(logger, globopts['OutputWeights'.lower()], jobdir)
                    avro = output.AvroWriter(globopts['AvroSchemasWeights'.lower()], filename)
                    ret, excep = avro.write(weights)
                    if not ret:
                        logger.error('Customer:%s Job:%s %s' % (logger.customer, logger.job, repr(excep)))
                        raise SystemExit(1)

                if weights or write_empty:
                    custs = set([cust for job, cust in jobcust])
                    for cust in custs:
                        jobs = [job for job, lcust in jobcust if cust == lcust]
                        logger.info('Customer:%s Jobs:%s Sites:%d' % (confcust.get_custname(cust),
                                                                      jobs[0] if len(jobs) == 1 else '({0})'.format(','.join(jobs)),
                                                                      len(weights)))

        except input.ConnectorError:
            for job, cust in jobcust:
                jobdir = confcust.get_fulldir(cust, job)
                jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)
                if fixed_date:
                    output.write_state(sys.argv[0], jobstatedir, False,
                                       globopts['InputStateDays'.lower()],
                                       fixed_date.replace('-', '_'))
                else:
                    output.write_state(sys.argv[0], jobstatedir, False,
                                       globopts['InputStateDays'.lower()])


if __name__ == '__main__':
    main()
