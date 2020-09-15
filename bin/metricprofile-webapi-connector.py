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
import re
import sys

from argo_egi_connectors import input
from argo_egi_connectors import output
from argo_egi_connectors.log import Logger

from argo_egi_connectors.config import CustomerConf, Global
from argo_egi_connectors.helpers import filename_date, module_class_name, datestamp, date_check

logger = None

globopts = dict()
custname = ''
API_PATH = '/api/v2/metric_profiles'


class WebAPI(object):
    def __init__(self, customer, job, profiles, namespace, host, token):
        self.state = True
        self.customer = customer
        self.job = job
        self.host = host
        self.token = token
        self.profiles = profiles
        self.namespace = namespace

    def get_profiles(self):
        try:
            fetched_profiles = self._fetch()
            target_profiles = filter(lambda profile: profile['name'] in self.profiles, fetched_profiles)
            profile_list = list()

            if len(target_profiles) == 0:
                self.state = False
                logger.error('Customer:' + self.customer + ' Job:' + self.job + ': No profiles {0} were found!'.format(', '.join(self.profiles)))

                raise SystemExit(1)

            for profile in target_profiles:
                for service in profile['services']:
                    for metric in service['metrics']:
                        if self.namespace:
                            profile_name = '{0}.{1}'.format(self.namespace, profile['name'])
                        else:
                            profile_name = profile['name']
                        profile_list.append({
                            'profile': profile_name,
                            'metric': metric,
                            'service': service['service']
                        })

        except (KeyError, IndexError, AttributeError, TypeError) as e:
            self.state = False
            logger.error(module_class_name(self) + ' Customer:%s : Error parsing feed %s - %s' % (logger.customer,
                                                                                                  self.host + API_PATH,
                                                                                                  repr(e).replace('\'', '').replace('\"', '')))
            return []
        else:
            return self._format(profile_list)

    def _fetch(self):
        try:
            res = input.connection(logger, module_class_name(self), globopts,
                                   'https', self.host, API_PATH,
                                   custauth={'WebAPIToken'.lower(): self.token})
            if not res:
                raise input.ConnectorError()

            json_data = input.parse_json(logger, module_class_name(self),
                                         globopts, res, self.host + API_PATH)

            if not json_data or not json_data.get('data', False):
                raise input.ConnectorError()

            return json_data['data']

        except input.ConnectorError:
            self.state = False

    def _format(self, profile_list):
        profiles = []

        for p in profile_list:
            pt = dict()
            pt['metric'] = p['metric']
            pt['profile'] = p['profile']
            pt['service'] = p['service']
            profiles.append(pt)

        return profiles


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description='Fetch metric profile for every job of the customer')
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

    for cust in confcust.get_customers():
        custname = confcust.get_custname(cust)

        for job in confcust.get_jobs(cust):
            logger.customer = confcust.get_custname(cust)
            logger.job = job

            profiles = confcust.get_profiles(job)
            webapi_custopts = confcust.get_webapiopts(cust)
            webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
            webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')

            if not webapi_complete:
                logger.error('Customer:%s Job:%s %s options incomplete, missing %s' % (custname, logger.job, 'webapi', ' '.join(missopt)))
                continue

            webapi = WebAPI(custname, job, profiles, confcust.get_namespace(job),
                            webapi_opts['webapihost'],
                            webapi_opts['webapitoken'])
            fetched_profiles = webapi.get_profiles()

            jobdir = confcust.get_fulldir(cust, job)
            jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)

            ams_custopts = confcust.get_amsopts(cust)
            ams_opts = cglob.merge_opts(ams_custopts, 'ams')
            ams_complete, missopt = cglob.is_complete(ams_opts, 'ams')
            if not ams_complete:
                logger.error('Customer:%s %s options incomplete, missing %s' % (custname, 'ams', ' '.join(missopt)))
                continue

            if fixed_date:
                output.write_state(sys.argv[0], jobstatedir,
                                   webapi.state,
                                   globopts['InputStateDays'.lower()],
                                   fixed_date.replace('-', '_'))
            else:
                output.write_state(sys.argv[0], jobstatedir,
                                   webapi.state,
                                   globopts['InputStateDays'.lower()])

            if not webapi.state:
                continue

            if eval(globopts['GeneralPublishAms'.lower()]):
                if fixed_date:
                    partdate = fixed_date
                else:
                    partdate = datestamp(1).replace('_', '-')

                ams = output.AmsPublish(ams_opts['amshost'],
                                        ams_opts['amsproject'],
                                        ams_opts['amstoken'],
                                        ams_opts['amstopic'],
                                        confcust.get_jobdir(job),
                                        ams_opts['amsbulk'],
                                        ams_opts['amspacksinglemsg'],
                                        logger,
                                        int(globopts['ConnectionRetry'.lower()]),
                                        int(globopts['ConnectionTimeout'.lower()]))

                ams.send(globopts['AvroSchemasMetricProfile'.lower()], 'metric_profile',
                         partdate, fetched_profiles)

            if eval(globopts['GeneralWriteAvro'.lower()]):
                if fixed_date:
                    filename = filename_date(logger, globopts['OutputMetricProfile'.lower()], jobdir, fixed_date.replace('-', '_'))
                else:
                    filename = filename_date(logger, globopts['OutputMetricProfile'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasMetricProfile'.lower()], filename)
                ret, excep = avro.write(fetched_profiles)
                if not ret:
                    logger.error('Customer:%s Job:%s %s' % (logger.customer, logger.job, repr(excep)))
                    raise SystemExit(1)

            logger.info('Customer:' + custname + ' Job:' + job + ' Profiles:%s Tuples:%d' % (', '.join(profiles), len(fetched_profiles)))


if __name__ == '__main__':
    main()
