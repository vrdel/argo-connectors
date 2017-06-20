#!/bin/env python

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

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.helpers import filename_date, module_class_name, datestamp
from urlparse import urlparse

globopts = {}
logger = None

VAPORPI = 'https://operations-portal.egi.eu/vapor/downloadLavoisier/option/json/view/VAPOR_Ngi_Sites_Info'

class Vapor:
    def __init__(self, feed):
        self._o = urlparse(feed)
        self.state = True

    def getWeights(self):
        try:
            res = input.connection(logger, globopts, self._o.scheme, self._o.netloc, self._o.path,
                                module_class_name(self))
            json_data = input.parse_json(logger, res, self._o.scheme + '://' + self._o.netloc + self._o.path, module_class_name(self))

        except input.ConnectorError:
            self.state = False

        else:
            try:
                weights = dict()
                for ngi in json_data:
                    for site in ngi['site']:
                        key = site['id']
                        val = site['HEPSPEC2006']
                        weights[key] = val
                return weights
            except (KeyError, IndexError) as e:
                self.state = False
                logger.error(module_class_name(self) + ': Error parsing feed %s - %s' % (self._o.scheme + '://' + self._o.netloc + self._o.path,
                                                                                         repr(e).replace('\'','')))


def data_out(data):
    datawr = []
    for key in data:
        w = data[key]
        datawr.append({'type': 'hepspec', 'site': key, 'weight': w})
    return datawr


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description="""Fetch weights information from Gstat provider
                                                    for every job listed in customer.conf""")
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
    feeds = confcust.get_mapfeedjobs(sys.argv[0], deffeed=VAPORPI)

    for feed, jobcust in feeds.items():
        weights = Vapor(feed)
        datawr = None

        w = weights.getWeights()

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

            output.write_state(sys.argv[0], jobstatedir, weights.state, globopts['InputStateDays'.lower()])

            if not weights.state:
                continue

            datawr = data_out(w)
            if eval(globopts['GeneralPublishAms'.lower()]):
                ams = output.AmsPublish(ams_opts['amshost'],
                                        ams_opts['amsproject'],
                                        ams_opts['amstoken'],
                                        ams_opts['amstopic'],
                                        confcust.get_jobdir(job),
                                        ams_opts['amsbulk'])
                ret, excep = ams.send(globopts['AvroSchemasWeights'.lower()],
                                      'weights', datestamp().replace('_', '-'), datawr)
                if not ret:
                    logger.error(excep)
                    raise SystemExit(1)

            if eval(globopts['GeneralWriteAvro'.lower()]):
                filename = filename_date(logger, globopts['OutputWeights'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasWeights'.lower()], filename)
                ret, excep = avro.write(datawr)
                if not ret:
                    logger.error(excep)
                    raise SystemExit(1)

        if datawr:
            custs = set([cust for job, cust in jobcust])
            for cust in custs:
                jobs = [job for job, lcust in jobcust if cust == lcust]
                logger.info('Customer:%s Jobs:%d Sites:%d' % (cust, len(jobs), len(datawr)))


if __name__ == '__main__':
    main()
