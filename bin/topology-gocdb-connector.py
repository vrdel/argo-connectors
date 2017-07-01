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
import copy
import os
import sys

from argo_egi_connectors import input
from argo_egi_connectors import output
from argo_egi_connectors.log import SingletonLogger as Logger

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.helpers import filename_date, module_class_name, datestamp
from urlparse import urlparse

logger = Logger(os.path.basename(sys.argv[0]))

SERVENDPI = '/gocdbpi/private/?method=get_service_endpoint'
SITESPI = '/gocdbpi/private/?method=get_site'
SERVGROUPPI = '/gocdbpi/private/?method=get_service_group'

fetchtype = ''

globopts = {}
custname = ''

isok = True

class GOCDBReader:
    def __init__(self, feed, scopes):
        self._o = urlparse(feed)
        self.scopes = scopes if scopes else set(['NoScope'])
        for scope in self.scopes:
            code = "self.serviceList%s = dict(); " % scope
            code += "self.groupList%s = dict();" % scope
            code += "self.siteList%s = dict()" % scope
            exec code
        self.fetched = False
        self.state = True

    def getGroupOfServices(self):
        if not self.fetched:
            if not self.state or not self.loadDataIfNeeded():
                return []

        groups, gl = list(), list()

        for scope in self.scopes:
            code = "gl = gl + [value for key, value in self.groupList%s.iteritems()]" % scope
            exec code

        for d in gl:
            for service in d['services']:
                g = dict()
                g['type'] = fetchtype.upper()
                g['group'] = d['name']
                g['service'] = service['type']
                g['hostname'] = service['hostname']
                g['group_monitored'] = d['monitored']
                g['tags'] = {'scope' : d['scope'], \
                            'monitored' : '1' if service['monitored'] == "Y" else '0', \
                            'production' : '1' if service['production'] == "Y" else '0'}
                groups.append(g)

        return groups

    def getGroupOfGroups(self):
        if not self.fetched:
            if not self.state or not self.loadDataIfNeeded():
                return []

        groupofgroups, gl = list(), list()

        if fetchtype == "ServiceGroups":
            for scope in self.scopes:
                code = "gl = gl + [value for key, value in self.groupList%s.iteritems()]" % scope
                exec code
            for d in gl:
                g = dict()
                g['type'] = 'PROJECT'
                g['group'] = custname
                g['subgroup'] = d['name']
                g['tags'] = {'monitored' : '1' if d['monitored'] == 'Y' else '0',
                            'scope' : d['scope']}
                groupofgroups.append(g)
        else:
            gg = []
            for scope in self.scopes:
                code = "gg = gg + sorted([value for key, value in self.siteList%s.iteritems()], key=lambda s: s['ngi'])" % scope
                exec code

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
        if not self.fetched:
            if not self.state or not self.loadDataIfNeeded():
                return []

        groupofendpoints, ge = list(), list()
        for scope in self.scopes:
            code = "ge = ge + sorted([value for key, value in self.serviceList%s.iteritems()], key=lambda s: s['site'])" % scope
            exec code

        for gr in ge:
            g = dict()
            g['type'] = fetchtype.upper()
            g['group'] = gr['site']
            g['service'] = gr['type']
            g['hostname'] = gr['hostname']
            g['tags'] = {'scope' : gr['scope'], \
                         'monitored' : '1' if gr['monitored'] == "Y" else '0', \
                         'production' : '1' if gr['production'] == "Y" else '0'}
            groupofendpoints.append(g)

        return groupofendpoints

    def loadDataIfNeeded(self):
        scopequery = "'&scope='+scope"
        for scope in self.scopes:
            try:
                eval("self.getSitesInternal(self.siteList%s, %s)" % (scope, '' if scope == 'NoScope' else scopequery))
                eval("self.getServiceGroups(self.groupList%s, %s)" % (scope, '' if scope == 'NoScope' else scopequery))
                eval("self.getServiceEndpoints(self.serviceList%s, %s)" % (scope, '' if scope == 'NoScope' else scopequery))
                self.fetched = True
            except Exception:
                self.state = False
                return False

        return True

    def _get_xmldata(self, scope, pi):
        res = input.connection(logger, module_class_name(self), globopts,
                               self._o.scheme, self._o.netloc, pi + scope)
        doc = input.parse_xml(logger, module_class_name(self), globopts, res,
                              self._o.scheme + '://' + self._o.netloc + pi)
        return doc

    def getServiceEndpoints(self, serviceList, scope):
        try:
            doc = self._get_xmldata(scope, SERVENDPI)
            services = doc.getElementsByTagName('SERVICE_ENDPOINT')
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
                serviceList[serviceId]['scope'] = scope.split('=')[1]
                serviceList[serviceId]['sortId'] = serviceList[serviceId]['hostname'] + '-' + serviceList[serviceId]['type'] + '-' + serviceList[serviceId]['site']

        except input.ConnectorError as e:
            raise e

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as e:
            logger.error(module_class_name(self) + ': Error parsing feed %s - %s' % (self._o.scheme + '://' + self._o.netloc + SERVENDPI,
                                                                                     repr(e).replace('\'','').replace('\"', '')))
            raise e

    def getSitesInternal(self, siteList, scope):
        try:
            doc = self._get_xmldata(scope, SITESPI)
            sites = doc.getElementsByTagName('SITE')
            for site in sites:
                siteName = site.getAttribute('NAME')
                if siteName not in siteList:
                    siteList[siteName] = {'site': siteName}
                siteList[siteName]['infrastructure'] = site.getElementsByTagName('PRODUCTION_INFRASTRUCTURE')[0].childNodes[0].data
                siteList[siteName]['certification'] = site.getElementsByTagName('CERTIFICATION_STATUS')[0].childNodes[0].data
                siteList[siteName]['ngi'] = site.getElementsByTagName('ROC')[0].childNodes[0].data
                siteList[siteName]['scope'] = scope.split('=')[1]

        except input.ConnectorError as e:
            raise e

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as e:
            logger.error(module_class_name(self) + ': Error parsing feed %s - %s' % (self._o.scheme + '://' + self._o.netloc + SITESPI,
                                                                                     repr(e).replace('\'','').replace('\"', '')))
            raise e

    def getServiceGroups(self, groupList, scope):
        try:
            doc = self._get_xmldata(scope, SERVGROUPPI)
            groups = doc.getElementsByTagName('SERVICE_GROUP')
            for group in groups:
                groupId = group.getAttribute('PRIMARY_KEY')
                if groupId not in groupList:
                    groupList[groupId] = {}
                groupList[groupId]['name'] = group.getElementsByTagName('NAME')[0].childNodes[0].data
                groupList[groupId]['monitored'] = group.getElementsByTagName('MONITORED')[0].childNodes[0].data
                groupList[groupId]['scope'] = scope.split('=')[1]
                groupList[groupId]['services'] = []
                services = group.getElementsByTagName('SERVICE_ENDPOINT')
                for service in services:
                    serviceDict = {}
                    serviceDict['hostname'] = service.getElementsByTagName('HOSTNAME')[0].childNodes[0].data
                    serviceDict['type'] = service.getElementsByTagName('SERVICE_TYPE')[0].childNodes[0].data
                    serviceDict['monitored'] = service.getElementsByTagName('NODE_MONITORED')[0].childNodes[0].data
                    serviceDict['production'] = service.getElementsByTagName('IN_PRODUCTION')[0].childNodes[0].data
                    groupList[groupId]['services'].append(serviceDict)

        except input.ConnectorError as e:
            raise e

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as e:
            logger.error(module_class_name(self) + ': Error parsing feed %s - %s' % (self._o.scheme + '://' + self._o.netloc + SERVGROUPPI,
                                                                                     repr(e).replace('\'','').replace('\"', '')))
            raise e


class TopoFilter(object):
    def __init__(self, gg, ge, ggfilter, gefilter):
        self.gg = gg
        self.ge = ge
        self.ggfilter = copy.copy(ggfilter)
        self.gefilter = copy.copy(gefilter)
        self.sitefilter = self.extract_filter('site', self.ggfilter)
        self.ngifilter = self.extract_filter('ngi', self.ggfilter)
        self.topofilter()

    def topofilter(self):
        if self.sitefilter:
            self.gg = filter(lambda e: e['subgroup'].lower() in self.sitefilter, self.gg)

        if self.ngifilter:
            self.gg = filter(lambda e: e['group'].lower() in self.ngifilter, self.gg)

        if self.ggfilter:
            self.gg = self.filter_tags(self.ggfilter, self.gg)

        allsubgroups = set([e['subgroup'] for e in self.gg])
        if allsubgroups:
            self.ge = filter(lambda e: e['group'] in allsubgroups, self.ge)

        if self.gefilter:
            self.ge = self.filter_tags(self.gefilter, self.ge)

    def extract_filter(self, tag, ggtags):
        gg = None
        if tag.lower() in [t.lower() for t in ggtags.iterkeys()]:
            for k, v in ggtags.iteritems():
                if tag.lower() in k.lower():
                    gg = ggtags[k]
                    key = k
            ggtags.pop(key)
            if isinstance(gg, list):
                gg = [t.lower() for t in gg]
            else:
                gg = gg.lower()

        return gg

    def filter_tags(self, tags, listofelem):
        for attr in tags.keys():
            def getit(elem):
                value = elem['tags'][attr.lower()]
                if value == '1': value = 'Y'
                elif value == '0': value = 'N'
                if isinstance(tags[attr], list):
                    for a in tags[attr]:
                        if value.lower() == a.lower():
                            return True
                else:
                    if value.lower() == tags[attr].lower():
                        return True
            try:
                listofelem = filter(getit, listofelem)
            except KeyError as e:
                logger.error('Wrong tags specified: %s' % e)
        return listofelem


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description="""Fetch entities (ServiceGroups, Sites, Endpoints)
                                                    from GOCDB for every customer and job listed in customer.conf and write them
                                                    in an appropriate place""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()
    group_endpoints, group_groups = [], []


    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    feeds = confcust.get_mapfeedjobs(sys.argv[0], 'GOCDB', deffeed='https://goc.egi.eu/gocdbpi/')

    for feed, jobcust in feeds.items():
        scopes = confcust.get_feedscopes(feed, jobcust)
        gocdb = GOCDBReader(feed, scopes)

        for job, cust in jobcust:
            jobdir = confcust.get_fulldir(cust, job)
            jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)

            global fetchtype, custname
            fetchtype = confcust.get_gocdb_fetchtype(job)
            custname = confcust.get_custname(cust)

            ams_custopts = confcust.get_amsopts(cust)
            ams_opts = cglob.merge_opts(ams_custopts, 'ams')
            ams_complete, missopt = cglob.is_complete(ams_opts, 'ams')
            if not ams_complete:
                logger.error('Customer:%s %s options incomplete, missing %s' % (custname, 'ams', ' '.join(missopt)))
                continue

            if fetchtype == 'ServiceGroups':
                group_endpoints = gocdb.getGroupOfServices()
            else:
                group_endpoints = gocdb.getGroupOfEndpoints()
            group_groups = gocdb.getGroupOfGroups()

            output.write_state(sys.argv[0], jobstatedir, gocdb.state, globopts['InputStateDays'.lower()])

            if not gocdb.state:
                continue

            numge = len(group_endpoints)
            numgg = len(group_groups)

            ggtags = confcust.get_gocdb_ggtags(job)
            getags = confcust.get_gocdb_getags(job)
            tf = TopoFilter(group_groups, group_endpoints, ggtags, getags)
            group_groups = tf.gg
            group_endpoints = tf.ge

            if eval(globopts['GeneralPublishAms'.lower()]):
                ams = output.AmsPublish(ams_opts['amshost'],
                                        ams_opts['amsproject'],
                                        ams_opts['amstoken'],
                                        ams_opts['amstopic'],
                                        confcust.get_jobdir(job),
                                        ams_opts['amsbulk'],
                                        logger,
                                        int(globopts['ConnectionRetry'.lower()]),
                                        int(globopts['ConnectionTimeout'.lower()]))

                ams.send(globopts['AvroSchemasTopologyGroupOfGroups'.lower()],
                         'group_groups', datestamp().replace('_', '-'),
                         group_groups)

                ams.send(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()],
                         'group_endpoints', datestamp().replace('_', '-'),
                         group_endpoints)

            if eval(globopts['GeneralWriteAvro'.lower()]):
                filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
                ret, excep = avro.write(group_groups)
                if not ret:
                    logger.error(excep)
                    raise SystemExit(1)

                filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename)
                ret, excep = avro.write(group_endpoints)
                if not ret:
                    logger.error(excep)
                    raise SystemExit(1)

            logger.info('Customer:'+custname+' Job:'+job+' Fetched Endpoints:%d' % (numge) +' Groups(%s):%d' % (fetchtype, numgg))
            if getags or ggtags:
                selstr = 'Customer:%s Job:%s Selected ' % (custname, job)
                selge, selgg = '', ''
                if getags:
                    for key, value in getags.items():
                        if isinstance(value, list):
                            value = '['+','.join(value)+']'
                        selge += '%s:%s,' % (key, value)
                    selstr += 'Endpoints(%s):' % selge[:len(selge) - 1]
                    selstr += '%d ' % (len(group_endpoints))
                if ggtags:
                    for key, value in ggtags.items():
                        if isinstance(value, list):
                            value = '['+','.join(value)+']'
                        selgg += '%s:%s,' % (key, value)
                    selstr += 'Groups(%s):' % selgg[:len(selgg) - 1]
                    selstr += '%d' % (len(group_groups))

                logger.info(selstr)


if __name__ == '__main__':
    main()
