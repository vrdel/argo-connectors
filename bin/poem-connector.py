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

from argo_egi_connectors.config import CustomerConf, PoemConf, Global
from argo_egi_connectors.helpers import filename_date, module_class_name, datestamp

logger = Logger(os.path.basename(sys.argv[0]))

globopts, poemopts = {}, {}
cpoem = None
custname = ''

MIPAPI = '/poem/api/0.2/json/metrics_in_profiles?vo_name='

class PoemReader:
    def __init__(self, noprefilter):
        self._nopf = noprefilter
        self.state = True

    def getProfiles(self):
        filteredProfiles = re.split('\s*,\s*', poemopts['FetchProfilesList'.lower()])
        availableVOs = [vo for k, v in cpoem.get_servers().items() for vo in v]

        try:
            validProfiles = self.loadValidProfiles(filteredProfiles)

            ngiall = cpoem.get_allngi()

            profileList = []
            profileListAvro = []

            for profile in validProfiles.values():
                for metric in profile['metrics']:
                    profileListAvro.append({'profile' : profile['namespace'] + '.' + profile['name'], \
                                            'metric' : metric['name'], \
                                            'service' : metric['service_flavour'], \
                                            'vo' : profile['vo'], \
                                            'fqan' : metric['fqan']})

            if not self._nopf:
                nummoninst = 0

                for server, profiles in ngiall.items():
                    ngis = ['ALL']
                    servers = [server]
                    defaultProfiles = profiles

                    for vo in availableVOs:
                        serverProfiles = []
                        if len(defaultProfiles) > 0:
                            serverProfiles = defaultProfiles
                        else:
                            serverProfiles = self.loadProfilesFromServer(servers[0], vo, filteredProfiles).keys()

                    for profile in serverProfiles:
                        if profile.upper() in validProfiles.keys():
                            for ngi in ngis:
                                for server in servers:
                                    profileList.extend(self.createProfileEntries(server, ngi, validProfiles[profile.upper()]))

        except (KeyError, IndexError, AttributeError, TypeError) as e:
            self.state = False
            logger.error(module_class_name(self) + ': Error parsing feed %s - %s' % (self._urlfeed,
                                                                                     repr(e).replace('\'','').replace('\"', '')))
            return [], []
        else:
            return profileList if profileList else [], profileListAvro

    def loadValidProfiles(self, filteredProfiles):
        validProfiles = dict()

        try:
            for url, vos in cpoem.get_servers().items():
                for vo in vos:
                    serverProfiles = self.loadProfilesFromServer(url, vo, filteredProfiles)
                    for profile in serverProfiles.keys():
                        if not profile in validProfiles.keys():
                            validProfiles[profile] = serverProfiles[profile]
                            validProfiles[profile]['vo'] = vo

        except Exception as e:
            raise e

        else:
            return validProfiles

    def loadProfilesFromServer(self, server, vo, filterProfiles):
        validProfiles = dict()

        doFilterProfiles = False
        if len(filterProfiles) > 0:
            doFilterProfiles = True

        if not server.startswith('http'):
            server = 'https://' + server

        self._urlfeed = server + MIPAPI + vo
        o = urlparse.urlparse(self._urlfeed, allow_fragments=True)

        try:
            assert o.scheme != '' and o.netloc != '' and o.path != ''
        except AssertionError:
            logger.error('Invalid POEM PI URL: %s' % (self._urlfeed))
            raise SystemExit(1)

        logger.info('Server:%s VO:%s' % (o.netloc, vo))

        try:
            res = input.connection(logger, globopts, o.scheme, o.netloc,
                                o.path + '?' + o.query,
                                module_class_name(self))
            json_data = input.parse_json(logger, res, self._urlfeed, module_class_name(self))

        except input.ConnectorError:
            self.state = False

        else:
            try:
                for profile in json_data[0]['profiles']:
                    if not doFilterProfiles or profile['namespace'].upper()+'.'+profile['name'] in filterProfiles:
                        validProfiles[profile['namespace'].upper()+'.'+profile['name']] = profile

            except Exception as e:
                raise e

            else:
                return validProfiles

    def createProfileEntries(self, server, ngi, profile):
        entries = list()
        try:
            for metric in profile['metrics']:
                entry = dict()
                entry["profile"] = profile['namespace']+'.'+profile['name']
                entry["service"] = metric['service_flavour']
                entry["metric"] = metric['name']
                entry["server"] = server
                entry["ngi"] = ngi
                entry["vo"] = profile['vo']
                entry["fqan"] = metric['fqan']
                entries.append(entry)
        except Exception as e:
            raise e
        else:
            return entries


class PrefilterPoem:
    def __init__(self):
        self.outputFileFormat = '%s\001%s\001%s\001%s\001%s\001%s\001%s\r\n'

    def writeProfiles(self, profiles, fname):
        outFile = open(fname, 'w')
        moninstance = set()
        for p in profiles:
            moninstance.add(p['server'])
            outFile.write(self.outputFileFormat % ( p['server'],
                       p['ngi'],
                       p['profile'],
                       p['service'],
                       p['metric'],
                       p['vo'],
                       p['fqan']))
        outFile.close();

        logger.info('POEM file(%s): Expanded profiles for %d monitoring instances' % (fname, len(moninstance) + 1))


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
    parser.add_argument('-np', dest='noprefilter', help='do not write POEM expanded profiles for prefilter', required=False, action='store_true')
    parser.add_argument('-p', dest='poemconf', nargs=1, metavar='poem-connector.conf', help='path to poem-connector configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()

    logger = Logger(os.path.basename(sys.argv[0]))

    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()

    servers = {'PoemServer': ['Host', 'VO']}
    filterprofiles = {'FetchProfiles': ['List']}
    prefilterdata = {'PrefilterData': ['AllowedNGI', 'AllowedNGIProfiles', 'AllNGI', 'AllNGIProfiles']}
    global cpoem, poemopts
    confpath = args.poemconf[0] if args.poemconf else None
    cpoem = PoemConf(confpath, servers, filterprofiles, prefilterdata)
    poemopts = cpoem.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])

    readerInstance = PoemReader(args.noprefilter)
    ps, psa = readerInstance.getProfiles()

    if not args.noprefilter and ps:
        poempref = PrefilterPoem()
        preffname = filename_date(logger, globopts['PrefilterPoemExpandedProfiles'.lower()], '')
        poempref.writeProfiles(ps, preffname)

    for cust in confcust.get_customers():
        # write profiles

        custname = confcust.get_custname(cust)

        for job in confcust.get_jobs(cust):
            jobdir = confcust.get_fulldir(cust, job)
            jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)

            ams_custopts = confcust.get_amsopts(cust)
            ams_opts = cglob.merge_opts(ams_custopts, 'ams')
            ams_complete, missopt = cglob.is_complete(ams_opts, 'ams')
            if not ams_complete:
                logger.error('Customer:%s %s options incomplete, missing %s' % (custname, 'ams', ' '.join(missopt)))
                continue

            output.write_state(sys.argv[0], jobstatedir, readerInstance.state, globopts['InputStateDays'.lower()])

            if not readerInstance.state:
                continue

            profiles = confcust.get_profiles(job)
            lfprofiles = gen_outprofiles(psa, profiles)

            if eval(globopts['GeneralPublishAms'.lower()]):
                ams = output.AmsPublish(ams_opts['amshost'],
                                        ams_opts['amsproject'],
                                        ams_opts['amstoken'],
                                        ams_opts['amstopic'],
                                        confcust.get_jobdir(job),
                                        ams_opts['amsbulk'])
                i = 1
                while i <= int(globopts['ConnectionRetry'.lower()]):
                    ret, excep = ams.send(globopts['AvroSchemasPoem'.lower()],
                                        'metric_profile', datestamp().replace('_', '-'), lfprofiles)
                    if not ret:
                        if i == int(globopts['ConnectionRetry'.lower()]):
                            logger.error(excep)
                            raise SystemExit(1)
                        else:
                            logger.warn('Try:%d AMS publish' % i)
                    elif ret:
                        break
                    i += 1

            if eval(globopts['GeneralWriteAvro'.lower()]):
                filename = filename_date(logger, globopts['OutputPoem'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasPoem'.lower()], filename)
                ret, excep = avro.write(lfprofiles)
                if not ret:
                    logger.error(excep)
                    raise SystemExit(1)

            logger.info('Customer:'+custname+' Job:'+job+' Profiles:%s Tuples:%d' % (','.join(profiles), len(lfprofiles)))


if __name__ == '__main__':
    main()
