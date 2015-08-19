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

import urllib
import datetime
import xml.dom.minidom
import httplib
import sys
import os
import socket
import copy
from exceptions import AssertionError

from argo_egi_connectors.writers import AvroWriter, Logger
from argo_egi_connectors.config import Global, CustomerConf


LegMapServType = {'SRM' : 'SRMv2'}
fetchtype = ''

globopts = {}
logger = None

class GOCDBReader:
    def __init__(self, feed):
        self.gocdbUrl = feed
        self.gocdbHost = self._getHostFeed(feed)
        self.hostKey = globopts['AuthenticationHostKey'.lower()]
        self.hostCert = globopts['AuthenticationHostCert'.lower()]
        self.siteListEGI, self.siteListLocal = dict(), dict()
        self.serviceListEGI, self.serviceListLocal = dict(), dict()
        self.groupListEGI, self.groupListLocal = dict(), dict()

    def _getHostFeed(self, feed):
        host = feed
        if "https://" in feed:
            host = feed.split("https://")[1]
            if "/" in host:
                host = host.split('/')[0]
        return host

    def getGroupOfServices(self):
        self.loadDataIfNeeded()

        groups = list()
        for d in self.groupListEGI, self.groupListLocal:
            key, group = d.iteritems()
            for service in group['services']:
                g = dict()
                g['type'] = fetchtype.upper()
                g['group'] = group['name']
                g['service'] = service['type']
                g['hostname'] = service['hostname']
                g['group_monitored'] = group['monitored']
                g['tags'] = {'scope' : group['scope'], \
                            'monitored' : 1 if service['monitored'] == "Y" else 0, \
                            'production' : 1 if service['production'] == "Y" else 0}
                groups.append(g)

        return groups

    def getGroupOfGroups(self):
        self.loadDataIfNeeded()

        groupofgroups = list()

        if fetchtype == "ServiceGroups":
            for d in self.groupListEGI, self.groupListLocal:
                key, value = d.iteritems()
                g = dict()
                g['type'] = 'PROJECT'
                g['group'] = 'EGI'
                g['subgroup'] = value['name']
                g['tags'] = {'monitored' : 1 if value['monitored'] == 'Y' else 0,
                            'scope' : value['scope']}
                groupofgroups.append(g)
        else:
            gg = sorted([value for d in self.siteListEGI, self.siteListLocal for key, value in d.iteritems()],
                                        key=lambda s: s['ngi'])

            for gr in gg:
                g = dict()
                g['type'] = 'NGI'
                g['group'] = gr['ngi']
                g['subgroup'] = gr['site']
                g['tags'] = {'certification' : gr['certification'], \
                             'scope' : gr['scope'], \
                             'infrastructure' : gr['infrastructure']}

                groupofgroups.append(g)

        return groupofgroups

    def getGroupOfEndpoints(self):
        self.loadDataIfNeeded()

        groupofendpoints = list()
        ge = sorted([value for d in self.serviceListEGI, self.serviceListLocal for key, value in d.iteritems()],
                                 key=lambda s: s['site'])

        for gr in ge:
            g = dict()
            g['type'] = fetchtype.upper()
            g['group'] = gr['site']
            g['service'] = gr['type']
            g['hostname'] = gr['hostname']
            g['tags'] = {'scope' : gr['scope'], \
                         'monitored' : 1 if gr['monitored'] == "Y" else 0, \
                         'production' : 1 if gr['production'] == "Y" else 0}
            groupofendpoints.append(g)

        return groupofendpoints

    def loadDataIfNeeded(self):
        try:
            if len(self.siteListEGI) == 0:
                self.getSitesInternal(self.siteListEGI, 'EGI')
                self.getSitesInternal(self.siteListLocal, 'Local')

            if len(self.serviceListEGI) == 0:
                self.getServiceEndpoints(self.serviceListEGI, 'EGI')
                self.getServiceEndpoints(self.serviceListLocal, 'Local')

            if len(self.groupListEGI) == 0:
                self.getServiceGroups(self.groupListEGI, 'EGI')
                self.getServiceGroups(self.groupListLocal, 'Local')
        except (socket.error, httplib.HTTPException) as e:
            logger.error('Connection to GOCDB failed: ' + str(e))
            raise SystemExit(1)

    def getServiceEndpoints(self, serviceList, scope):
        try:
            conn = httplib.HTTPSConnection(self.gocdbHost, 443, self.hostKey, self.hostCert)
            conn.request('GET', '/gocdbpi/private/?method=get_service_endpoint&scope=' + scope)
            res = conn.getresponse()
            if res.status == 200:
                doc = xml.dom.minidom.parseString(res.read())
                services = doc.getElementsByTagName('SERVICE_ENDPOINT')
                assert len(services) > 0
                for service in services:
                    serviceId = ''
                    if service.getAttributeNode('PRIMARY_KEY'):
                        serviceId = str(service.attributes['PRIMARY_KEY'].value)
                    if serviceId not in serviceList:
                        serviceList[serviceId] = {}
                    serviceList[serviceId]['hostname'] = service.getElementsByTagName('HOSTNAME')[0].childNodes[0].data
                    serviceList[serviceId]['type'] = service.getElementsByTagName('SERVICE_TYPE')[0].childNodes[0].data
                    serviceList[serviceId]['monitored'] = service.getElementsByTagName('NODE_MONITORED')[0].childNodes[0].data
                    serviceList[serviceId]['production'] = service.getElementsByTagName('IN_PRODUCTION')[0].childNodes[0].data
                    serviceList[serviceId]['site'] = service.getElementsByTagName('SITENAME')[0].childNodes[0].data
                    serviceList[serviceId]['roc'] = service.getElementsByTagName('ROC_NAME')[0].childNodes[0].data
                    serviceList[serviceId]['scope'] = scope
                    serviceList[serviceId]['sortId'] = serviceList[serviceId]['hostname'] + '-' + serviceList[serviceId]['type'] + '-' + serviceList[serviceId]['site']
            else:
                logger.error('GOCDBReader.getServiceEndpoints(): HTTP response: %s %s' % (str(res.status), res.reason))
                raise SystemExit(1)
        except AssertionError:
            logger.error("GOCDBReader.getServiceEndpoints():", "Error parsing feed")
            raise SystemExit(1)

    def getSitesInternal(self, siteList, scope):
        try:
            conn = httplib.HTTPSConnection(self.gocdbHost, 443, self.hostKey, self.hostCert)
            conn.request('GET', '/gocdbpi/private/?method=get_site&scope=' + scope)
            res = conn.getresponse()
            if res.status == 200:
                dom = xml.dom.minidom.parseString(res.read())
                sites = dom.getElementsByTagName('SITE')
                assert len(sites) > 0
                for site in sites:
                    siteName = site.getAttribute('NAME')
                    if siteName not in siteList:
                        siteList[siteName] = {'site': siteName}
                    siteList[siteName]['infrastructure'] = site.getElementsByTagName('PRODUCTION_INFRASTRUCTURE')[0].childNodes[0].data
                    siteList[siteName]['certification'] = site.getElementsByTagName('CERTIFICATION_STATUS')[0].childNodes[0].data
                    siteList[siteName]['ngi'] = site.getElementsByTagName('ROC')[0].childNodes[0].data
                    siteList[siteName]['scope'] = scope
            else:
                logger.error('GOCDBReader.getSitesInternal(): HTTP response: %s %s' % (str(res.status), res.reason))
                raise SystemExit(1)
        except AssertionError:
            logger.error("GOCDBReader.getSitesInternal():", "Error parsing feed")
            raise SystemExit(1)

    def getServiceGroups(self, groupList, scope):
        try:
            conn = httplib.HTTPSConnection(self.gocdbHost, 443, self.hostKey, self.hostCert)
            conn.request('GET', '/gocdbpi/private/?method=get_service_group&scope=' + scope)
            res = conn.getresponse()
            if res.status == 200:
                doc = xml.dom.minidom.parseString(res.read())
                groups = doc.getElementsByTagName('SERVICE_GROUP')
                assert len(groups) > 0
                for group in groups:
                    groupId = group.getAttribute('PRIMARY_KEY')
                    if groupId not in groupList:
                        groupList[groupId] = {}
                    groupList[groupId]['name'] = group.getElementsByTagName('NAME')[0].childNodes[0].data
                    groupList[groupId]['monitored'] = group.getElementsByTagName('MONITORED')[0].childNodes[0].data
                    groupList[groupId]['scope'] = scope
                    groupList[groupId]['services'] = []
                    services = group.getElementsByTagName('SERVICE_ENDPOINT')
                    for service in services:
                        serviceDict = {}
                        serviceDict['hostname'] = service.getElementsByTagName('HOSTNAME')[0].childNodes[0].data
                        serviceDict['type'] = service.getElementsByTagName('SERVICE_TYPE')[0].childNodes[0].data
                        serviceDict['monitored'] = service.getElementsByTagName('NODE_MONITORED')[0].childNodes[0].data
                        serviceDict['production'] = service.getElementsByTagName('IN_PRODUCTION')[0].childNodes[0].data
                        groupList[groupId]['services'].append(serviceDict)
            else:
                logger.error('GOCDBReader.getServiceGroups(): HTTP response: %s %s' % (str(res.status), res.reason))
                raise SystemExit(1)
        except AssertionError:
            logger.error("GOCDBReader.getServiceGroups():", "Error parsing feed")
            raise SystemExit(1)

def filter_by_tags(tags, listofelem):
    for attr in tags.keys():
        def getit(elem):
            value = elem['tags'][attr.lower()]
            if isinstance(value, int):
                value = 'Y' if value else 'N'
            if value.lower() == tags[attr].lower():
                return True
        listofelem = filter(getit, listofelem)
    return listofelem

def main():
    group_endpoints, group_groups = [], []

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
    feeds = confcust.get_mapfeedjobs(sys.argv[0], 'GOCDB', deffeed='https://goc.egi.eu/gocdbpi/')

    timestamp = datetime.datetime.utcnow().strftime('%Y_%m_%d')

    for feed, jobcust in feeds.items():
        gocdb = GOCDBReader(feed)

        for job, cust in jobcust:
            jobdir = confcust.get_fulldir(cust, job)
            global fetchtype
            fetchtype = confcust.get_gocdb_fetchtype(job)

            if fetchtype == 'ServiceGroups':
                group_endpoints = gocdb.getGroupOfServices()
            else:
                group_endpoints = gocdb.getGroupOfEndpoints()
            group_groups = gocdb.getGroupOfGroups()

            numge = len(group_endpoints)
            numgg = len(group_groups)

            ggtags = confcust.get_gocdb_ggtags(job)
            if ggtags:
                group_groups = filter_by_tags(ggtags, group_groups)
            filename = jobdir+globopts['OutputTopologyGroupOfGroups'.lower()] % timestamp
            avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename,
                            group_groups, os.path.basename(sys.argv[0]), logger)
            avro.write()

            gelegmap = []
            for g in group_endpoints:
                if g['service'] in LegMapServType.keys():
                    gelegmap.append(copy.copy(g))
                    gelegmap[-1]['service'] = LegMapServType[g['service']]
            getags = confcust.get_gocdb_getags(job)
            if getags:
                group_endpoints = filter_by_tags(getags, group_endpoints)
            filename = jobdir+globopts['OutputTopologyGroupOfEndpoints'.lower()] % timestamp
            avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename,
                            group_endpoints + gelegmap, os.path.basename(sys.argv[0]), logger)
            avro.write()

            logger.info('Job:'+job+' Fetched Endpoints:%d' % (numge + len(gelegmap))+' Groups(%s):%d' % (fetchtype, numgg))
            if getags or ggtags:
                selstr = 'Job:%s Selected ' % (job)
                selge, selgg = '', ''
                if getags:
                    for key, value in getags.items():
                        selge += '%s:%s,' % (key, value)
                    selstr += 'Endpoints(%s):' % selge[:len(selge) - 1]
                    selstr += '%d ' % (len(group_endpoints) + len(gelegmap))
                if ggtags:
                    for key, value in ggtags.items():
                        selgg += '%s:%s,' % (key, value)
                    selstr += 'Groups(%s):' % selgg[:len(selgg) - 1]
                    selstr += '%d' % (len(group_groups))

                logger.info(selstr)
main()
