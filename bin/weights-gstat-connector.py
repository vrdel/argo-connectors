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
import datetime
import json
import os
import re
import socket
import sys
import urllib2

from OpenSSL.SSL import Error as SSLError
from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.tools import verify_cert, errmsg_from_excp, gen_fname_repdate, make_connection
from argo_egi_connectors.writers import AvroWriter
from argo_egi_connectors.writers import SingletonLogger as Logger
from avro.datafile import DataFileReader
from avro.io import DatumReader
from urlparse import urlparse

globopts = {}
logger = None

class GstatReader:
    def __init__(self, feed):
        self._o = urlparse(feed)

    def getWeights(self):
        res = make_connection(logger, globopts, self._o.scheme, self._o.netloc, self._o.path,
                              "GstatReader.getWeights()):")
        if res.status == 200:
            json_data = json.loads(res.read())
            weights = dict()
            for site in json_data:
                key = site['Sitename']
                val = site['HEPSPEC06']
                weights[key] = val
            return weights
        else:
            logger.error('GStatReader.getWeights(): HTTP response: %s %s' % (str(res.status), res.reason))
            raise SystemExit(1)

def gen_outdict(data):
    datawr = []
    for key in data:
        w = data[key]
        datawr.append({'type': 'hepspec', 'site': key, 'weight': w})
    return datawr

def loadOldData(filename):
    oldDataDict = dict()

    if not os.path.isfile(filename):
        return oldDataDict

    reader = DataFileReader(open(filename, "r"), DatumReader())
    for weight in reader:
        oldDataDict[weight["site"]] = weight["weight"]
    reader.close()

    return oldDataDict

def main():
    parser = argparse.ArgumentParser(description="""Fetch weights information from Gstat provider
                                                    for every job listed in customer.conf""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()

    global logger
    logger = Logger(os.path.basename(sys.argv[0]))

    certs = {'Authentication': ['HostKey', 'HostCert', 'CAPath', 'VerifyServerCert']}
    schemas = {'AvroSchemas': ['Weights']}
    output = {'Output': ['Weights']}
    conn = {'Connection': ['Timeout', 'Retry']}
    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(confpath, schemas, output, certs, conn)
    global globopts
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    feeds = confcust.get_mapfeedjobs(sys.argv[0], deffeed='http://gstat2.grid.sinica.edu.tw/gstat/summary/json/')

    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')
    oldDate = datetime.datetime.utcnow()

    for feed, jobcust in feeds.items():
        weights = GstatReader(feed)
        # load new data
        newData = dict()
        newData.update(weights.getWeights());

        # fill new list
        for key in newData:
            newVal = int(newData[key])
            if newVal <= 0:
                if key in oldData:
                    newVal = int(oldData[key])
            if key not in oldData:
                oldData[key] = str(newVal)
            newData[key] = str(newVal)

        for job, cust in jobcust:
            jobdir = confcust.get_fulldir(cust, job)

            oldFilename = gen_fname_repdate(logger, oldDate.strftime('%Y_%m_%d'), globopts['OutputWeights'.lower()], jobdir)
            i = 0
            oldDataExists = True
            while not os.path.isfile(oldFilename):
                oldDate = oldDate - datetime.timedelta(days=1)
                oldFilename = gen_fname_repdate(logger, oldDate.strftime('%Y_%m_%d'), globopts['OutputWeights'.lower()], jobdir)
                i = i + 1
                if i >= 30:
                    oldDataExists = False
                    break

            # load old data
            oldData = dict()
            if oldDataExists:
                oldData.update(loadOldData(gen_fname_repdate(logger, timestamp, globopts['OutputWeights'.lower()], jobdir)))

            # fill old list
            for key in oldData:
                oldVal = int(oldData[key])
                if oldVal <= 0:
                    if key in newData:
                        oldData[key] = newData[key]
                if key not in newData:
                    newData[key] = oldData[key]

            filename = gen_fname_repdate(logger, timestamp, globopts['OutputWeights'.lower()], jobdir)

            datawr = gen_outdict(newData)
            avro = AvroWriter(globopts['AvroSchemasWeights'.lower()], filename, datawr, os.path.basename(sys.argv[0]))
            avro.write()

            if oldDataExists:
                datawr = gen_outdict(oldData)
                avro = AvroWriter(globopts['AvroSchemasWeights'.lower()], filename, datawr, os.path.basename(sys.argv[0]))
                avro.write()

        custs = set([cust for job, cust in jobcust])
        for cust in custs:
            jobs = [job for job, lcust in jobcust if cust == lcust]
            logger.info('Customer:%s Jobs:%d Sites:%d' % (cust, len(jobs), len(datawr)))

main()
