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

from argo_egi_connectors.writers import AvroWriter, Logger
from argo_egi_connectors.config import Global, CustomerConf
from exceptions import AssertionError
import datetime
import httplib
import re
import sys
import socket
import os
import xml.dom.minidom
import copy

LegMapServType = {'SRM' : 'SRMv2', 'SRMv2': 'SRM'}
globopts = {}
logger = None

class VOReader:
    lendpoints= []
    lgroups = []

    def __init__(self, feed):
        host, path = self._host_path(feed)
        self._parse(host, path)

    def _host_path(self, feed):
        urlsplit = re.split('/*', feed)
        host = (urlsplit[0], urlsplit[1])
        path = '/'+'/'.join(urlsplit[2:])
        return host, path

    def _parse(self, host, path):
        try:
            if 'https' in host[0]:
                conn = httplib.HTTPSConnection(host[1], 443,
                                            globopts['AuthenticationHostKey'.lower()],
                                            globopts['AuthenticationHostCert'.lower()])
            elif 'http' in host[0]:
                conn = httplib.HTTPConnection(host[1])

        except (socket.error, httplib.HTTPException) as e:
            logger.error('Connection to %s failed: ' % (host) + str(e))
            raise SystemExit(1)

        conn.request('GET', path)
        res = conn.getresponse()
        try:
            if res.status == 200:
                dom = xml.dom.minidom.parseString(res.read())
                sites = dom.getElementsByTagName('atp_site')
                assert len(sites) > 0
                for site in sites:
                    gg = {}
                    subgroup = site.getAttribute('name')
                    assert subgroup != ''
                    groups = site.getElementsByTagName('group')
                    assert len(groups) > 0
                    for group in groups:
                        gg = {}
                        gg['group'] = group.getAttribute('name')
                        gg['type'] = group.getAttribute('type')
                        gg['subgroup'] = subgroup
                        self.lgroups.append(gg)

                    endpoints = site.getElementsByTagName('service')
                    assert len(endpoints) > 0
                    for endpoint in endpoints:
                        ge = {}
                        ge['group'] = subgroup
                        ge['hostname'] = endpoint.getAttribute('hostname')
                        ge['service'] = endpoint.getAttribute('flavour')
                        ge['type'] = 'SITES'
                        self.lendpoints.append(ge)
            else:
                logger.error('VOReader._parse(): Connection failed %s, HTTP response: %s %s' % (host[1], str(res.status), res.reason))
                raise SystemExit(1)
        except AssertionError:
            logger.error("Error parsing VO-feed %s" % ('//'.join(host)+path))
            raise SystemExit(1)

    def get_groupgroups(self):
        return self.lgroups

    def get_groupendpoints(self):
        return self.lendpoints


def main():
    global logger
    logger = Logger(os.path.basename(sys.argv[0]))

    certs = {'Authentication': ['HostKey', 'HostCert']}
    schemas = {'AvroSchemas': ['TopologyGroupOfEndpoints', 'TopologyGroupOfGroups']}
    output = {'Output': ['TopologyGroupOfEndpoints', 'TopologyGroupOfGroups']}
    cglob = Global(certs, schemas, output)
    global globopts
    globopts = cglob.parse()

    confcust = CustomerConf(sys.argv[0])
    confcust.parse()
    confcust.make_dirstruct()
    feeds = confcust.get_mapfeedjobs(sys.argv[0], 'VOFeed')

    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')

    for feed, jobcust in feeds.items():
        vo = VOReader(feed)

        for job, cust in jobcust:
            jobdir = confcust.get_fulldir(cust, job)

            filtlgroups = vo.get_groupgroups()
            numgg = len(filtlgroups)
            tags = confcust.get_vo_ggtags(job)
            if tags:
                def ismatch(elem):
                    values = tags['Type']
                    e = elem['type'].lower()
                    for val in values:
                        if e == val.lower():
                            return True
                filtlgroups = filter(ismatch, filtlgroups)

            filename = jobdir + globopts['OutputTopologyGroupOfGroups'.lower()] % timestamp
            avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename, filtlgroups,
                              os.path.basename(sys.argv[0]), logger)
            avro.write()

            filename = jobdir + globopts['OutputTopologyGroupOfEndpoints'.lower()] % timestamp
            gelegmap = []
            group_endpoints = vo.get_groupendpoints()
            numge = len(group_endpoints)
            for g in group_endpoints:
                if g['service'] in LegMapServType.keys():
                    gelegmap.append(copy.copy(g))
                    gelegmap[-1]['service'] = LegMapServType[g['service']]
            avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename, group_endpoints + gelegmap,
                                                                                       os.path.basename(sys.argv[0]), logger)
            avro.write()

            logger.info('Job:'+job+' Fetched Endpoints:%d' % (numge + len(gelegmap))+' Groups:%d' % (numgg))
            if tags:
                selstr = 'Job:%s Selected ' % (job)
                selgg = ''
                for key, value in tags.items():
                    selgg += '%s:%s,' % (key, ','.join(value))
                selstr += 'Groups(%s):' % selgg[:len(selgg) - 1]
                selstr += '%d' % (len(filtlgroups))

                logger.info(selstr)

main()
