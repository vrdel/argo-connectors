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
import os
import re
import sys
import urlparse

from argo_egi_connectors import input
from argo_egi_connectors import output
from argo_egi_connectors.log import Logger

from argo_egi_connectors.config import CustomerConf, Global
from argo_egi_connectors.helpers import filename_date, module_class_name, datestamp, date_check

logger = None

globopts, poemopts = {}, {}
cpoem = None
custname = ''

MIPAPI = '/poem/api/0.2/json/metrics_in_profiles?vo_name='

class PoemReader:
    def __init__(self):
        self.state = True

    def getProfiles(self, Profiles, PoemServer):

        try:
            validProfiles = self.loadValidProfiles(Profiles, PoemServer)

            profileListAvro = []

            for profile in validProfiles.values():
                for metric in profile['metrics']:
                    profileListAvro.append({'profile' : profile['namespace'] + '.' + profile['name'], \
                                            'metric' : metric['name'], \
                                            'service' : metric['service_flavour'], \
                                            'vo' : profile['vo'], \
                                            'fqan' : metric['fqan']})

        except (KeyError, IndexError, AttributeError, TypeError) as e:
            self.state = False
            logger.error(module_class_name(self) + ' Customer:%s : Error parsing feed %s - %s' % (logger.customer, self._urlfeed,
                                                                                     repr(e).replace('\'','').replace('\"', '')))
            return []
        else:
            return profileListAvro

    def loadValidProfiles(self, Profiles, PoemServers):
        validProfiles = dict()

        try:
            for url, vos in PoemServers.items():
                for vo in vos:
                    serverProfiles = self.loadProfilesFromServer(url, vo, Profiles)
                    for profile in serverProfiles.keys():
                        if not profile in validProfiles.keys():
                            validProfiles[profile] = serverProfiles[profile]
                            validProfiles[profile]['vo'] = vo

        except Exception as e:
            raise e

        else:
            return validProfiles

    def loadProfilesFromServer(self, server, vo, Profiles):
        validProfiles = dict()

        doFilterProfiles = False
        if len(Profiles) > 0:
            doFilterProfiles = True

        if not server.startswith('http'):
            server = 'https://' + server

        self._urlfeed = server + MIPAPI + vo
        o = urlparse.urlparse(self._urlfeed, allow_fragments=True)

        try:
            assert o.scheme != '' and o.netloc != '' and o.path != ''
        except AssertionError:
            logger.error('Customer:%s Invalid POEM PI URL: %s' % (logger.customer, self._urlfeed))
            raise SystemExit(1)

        logger.info('Customer:%s Server:%s VO:%s' % (logger.customer, o.netloc, vo))

        try:
            res = input.connection(logger, module_class_name(self), globopts,
                                   o.scheme, o.netloc, o.path + '?' + o.query)
            if not res:
                raise input.ConnectorError()

            json_data = input.parse_json(logger, module_class_name(self),
                                         globopts, res, self._urlfeed)

            if not json_data:
                raise input.ConnectorError()

        except input.ConnectorError:
            self.state = False

        else:
            try:
                for profile in json_data[0]['profiles']:
                    if not doFilterProfiles or profile['namespace'].upper()+'.'+profile['name'] in Profiles:
                        validProfiles[profile['namespace'].upper()+'.'+profile['name']] = profile

            except Exception as e:
                raise e

            else:
                return validProfiles

def gen_outprofiles(lprofiles, matched):
    lfprofiles = []

    for p in lprofiles:
        if p['profile'].split('.')[-1] in matched:
            pt = dict()
            pt['metric'] = p['metric']
            pt['profile'] = p['profile']
            pt['service'] = p['service']
            pt['tags'] = {'vo' : p['vo'], 'fqan' : p['fqan']}
            lfprofiles.append(pt)

    return lfprofiles


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description='Fetch POEM profile for every job of the customer and write POEM expanded profiles needed for prefilter for EGI customer')
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

    customers = set(map(lambda c: confcust.get_custname(c), confcust.get_customers()))
    customers = customers.pop() if len(customers) == 1 else '({0})'.format(','.join(customers))
    logger.customer = customers
    customers = confcust.get_customers()
    jobs = list()
    poemserver = dict()
    nsprofiles = list()
    for c in customers:
        jobs = jobs + confcust.get_jobs(c)
        for j in jobs:
            if confcust.get_poemserver_host(j) in poemserver:
                if isinstance(poemserver[confcust.get_poemserver_host(j)], (list,)):
                    poemserver[confcust.get_poemserver_host(j)] = poemserver[confcust.get_poemserver_host(j)] + \
                                                                  [confcust.get_poemserver_vo(j)]
                else:
                    val = [poemserver[confcust.get_poemserver_host(j)]]
                    poemserver[confcust.get_poemserver_host(j)] = val + [confcust.get_poemserver_vo(j)]
            else:
                poemserver[confcust.get_poemserver_host(j)] = confcust.get_poemserver_vo(j)
            nsprofiles = nsprofiles + [confcust.get_namespace(j) + '.' + s for s in confcust.get_profiles(j)]
    jobs = jobs.pop() if len(jobs) == 1 else '({0})'.format(','.join(jobs))
    logger.job = jobs

    readerInstance = PoemReader()
    psa = readerInstance.getProfiles(nsprofiles, poemserver)

    for cust in confcust.get_customers():
        # write profiles

        custname = confcust.get_custname(cust)

        for job in confcust.get_jobs(cust):
            logger.customer = confcust.get_custname(cust)
            logger.job = job

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
                                   readerInstance.state,
                                   globopts['InputStateDays'.lower()],
                                   fixed_date.replace('-', '_'))
            else:
                output.write_state(sys.argv[0], jobstatedir,
                                   readerInstance.state,
                                   globopts['InputStateDays'.lower()])

            if not readerInstance.state:
                continue

            profiles = confcust.get_profiles(job)
            lfprofiles = gen_outprofiles(psa, profiles)

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

                # ams.send(globopts['AvroSchemasPoem'.lower()], 'metric_profile',
                #          partdate, lfprofiles)

            if eval(globopts['GeneralWriteAvro'.lower()]):
                if fixed_date:
                    filename = filename_date(logger, globopts['OutputPoem'.lower()], jobdir, fixed_date.replace('-', '_'))
                else:
                    filename = filename_date(logger, globopts['OutputPoem'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasPoem'.lower()], filename)
                ret, excep = avro.write(lfprofiles)
                if not ret:
                    logger.error('Customer:%s Job:%s %s' % (logger.customer, logger.job, repr(excep)))
                    raise SystemExit(1)

            logger.info('Customer:'+custname+' Job:'+job+' Profiles:%s Tuples:%d' % (','.join(profiles), len(lfprofiles)))


if __name__ == '__main__':
    main()
