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
import copy
import os
import sys
import re

from argo_egi_connectors import input
from argo_egi_connectors import output
from argo_egi_connectors.log import Logger

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.helpers import filename_date, module_class_name, datestamp, date_check
from urllib.parse import urlparse

logger = None

SERVENDPI = '/gocdbpi/private/?method=get_service_endpoint'
SITESPI = '/gocdbpi/private/?method=get_site'
SERVGROUPPI = '/gocdbpi/private/?method=get_service_group'

fetchtype = ''

globopts = {}
custname = ''

isok = True


def rem_nonalpha(string):
    return re.sub(r'\W*', '', string)


def getText(nodelist):
    rc = []
    for node in nodelist:
        if node.nodeType == node.TEXT_NODE:
            rc.append(node.data)
    return ''.join(rc)


class GOCDBReader:
    def __init__(self, feed, scopes, paging=False, auth=None):
        self._o = urlparse(feed)
        self.scopes = scopes if scopes else set(['NoScope'])
        for scope in self.scopes:
            code = "self.serviceList%s = dict(); " % rem_nonalpha(scope)
            code += "self.groupList%s = dict();" % rem_nonalpha(scope)
            code += "self.siteList%s = dict()" % rem_nonalpha(scope)
            exec(code)
        self.fetched = False
        self.state = True
        self.paging = paging
        self.custauth = auth

    def getGroupOfServices(self, uidservtype=False):
        if not self.fetched:
            if not self.state or not self.loadDataIfNeeded():
                return []

        groups, gl = list(), list()

        for scope in self.scopes:
            loc = locals()
            code = "gl = gl + [value for key, value in self.groupList%s.items()]" % rem_nonalpha(scope)
            exec(code)
            gl = loc['gl']

        for d in gl:
            for service in d['services']:
                g = dict()
                g['type'] = fetchtype.upper()
                g['group'] = d['name']
                g['service'] = service['type']
                if uidservtype:
                    g['hostname'] = '{1}_{0}'.format(service['service_id'], service['hostname'])
                else:
                    g['hostname'] = service['hostname']
                g['tags'] = {'scope': d['scope'],
                             'monitored': '1' if service['monitored'].lower() == 'Y'.lower() or
                             service['monitored'].lower() == 'True'.lower() else '0',
                             'production': '1' if service['production'].lower() == 'Y'.lower() or
                             service['production'].lower() == 'True'.lower() else '0'}
                groups.append(g)

        return groups

    def getGroupOfGroups(self):
        if not self.fetched:
            if not self.state or not self.loadDataIfNeeded():
                return []

        groupofgroups, gl = list(), list()

        if fetchtype == "ServiceGroups":
            for scope in self.scopes:
                loc = locals()
                code = "gl = gl + [value for key, value in self.groupList%s.items()]" % rem_nonalpha(scope)
                exec(code)
                gl = loc['gl']

            for d in gl:
                g = dict()
                g['type'] = 'PROJECT'
                g['group'] = custname
                g['subgroup'] = d['name']
                g['tags'] = {'monitored': '1' if d['monitored'].lower() == 'Y'.lower() or
                             d['monitored'].lower() == 'True'.lower() else '0', 'scope': d['scope']}
                groupofgroups.append(g)
        else:
            gg = []
            for scope in self.scopes:
                loc = locals()
                code = "gg = gg + sorted([value for key, value in self.siteList%s.items()], key=lambda s: s['ngi'])" % rem_nonalpha(scope)
                exec(code)
                gg = loc['gg']

            for gr in gg:
                g = dict()
                g['type'] = 'NGI'
                g['group'] = gr['ngi']
                g['subgroup'] = gr['site']
                g['tags'] = {'certification': gr['certification'],
                             'scope': gr['scope'],
                             'infrastructure': gr['infrastructure']}

                groupofgroups.append(g)

        return groupofgroups

    def getGroupOfEndpoints(self, uidservtype=False):
        if not self.fetched:
            if not self.state or not self.loadDataIfNeeded():
                return []

        groupofendpoints, ge = list(), list()

        for scope in self.scopes:
            loc = locals()
            code = "ge = ge + sorted([value for key, value in self.serviceList%s.items()], key=lambda s: s['site'])" % rem_nonalpha(scope)
            exec(code)
            ge = loc['ge']

        for gr in ge:
            g = dict()
            g['type'] = fetchtype.upper()
            g['group'] = gr['site']
            g['service'] = gr['type']
            if uidservtype:
                g['hostname'] = '{1}_{0}'.format(gr['service_id'], gr['hostname'])
            else:
                g['hostname'] = gr['hostname']
            g['tags'] = {'scope': gr['scope'],
                         'monitored': '1' if gr['monitored'] == 'Y' or
                         gr['monitored'] == 'True' else '0',
                         'production': '1' if gr['production'] == 'Y' or
                         gr['production'] == 'True' else '0'}
            groupofendpoints.append(g)

        return groupofendpoints

    def loadDataIfNeeded(self):
        for scope in self.scopes:
            scopequery = "'&scope='+scope" if scope != 'NoScope' else "'&scope='"
            try:
                eval("self.getServiceEndpoints(self.serviceList%s, %s)" % (rem_nonalpha(scope), scopequery))
                eval("self.getServiceGroups(self.groupList%s, %s)" % (rem_nonalpha(scope), scopequery))
                eval("self.getSitesInternal(self.siteList%s, %s)" % (rem_nonalpha(scope), scopequery))
                self.fetched = True
            except Exception:
                self.state = False
                return False

        return True

    def _get_xmldata(self, scope, pi):
        res = input.connection(logger, module_class_name(self), globopts,
                               self._o.scheme, self._o.netloc, pi + scope, custauth=self.custauth)
        if not res:
            raise input.ConnectorError()

        doc = input.parse_xml(logger, module_class_name(self), globopts, res,
                              self._o.scheme + '://' + self._o.netloc + pi)
        return doc

    def _get_service_endpoints(self, serviceList, scope, doc):
        try:
            services = doc.getElementsByTagName('SERVICE_ENDPOINT')
            for service in services:
                serviceId = ''
                if service.getAttributeNode('PRIMARY_KEY'):
                    serviceId = str(service.attributes['PRIMARY_KEY'].value)
                if serviceId not in serviceList:
                    serviceList[serviceId] = {}
                serviceList[serviceId]['hostname'] = getText(service.getElementsByTagName('HOSTNAME')[0].childNodes)
                serviceList[serviceId]['type'] = getText(service.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                serviceList[serviceId]['monitored'] = getText(service.getElementsByTagName('NODE_MONITORED')[0].childNodes)
                serviceList[serviceId]['production'] = getText(service.getElementsByTagName('IN_PRODUCTION')[0].childNodes)
                serviceList[serviceId]['site'] = getText(service.getElementsByTagName('SITENAME')[0].childNodes)
                serviceList[serviceId]['roc'] = getText(service.getElementsByTagName('ROC_NAME')[0].childNodes)
                serviceList[serviceId]['service_id'] = serviceId
                serviceList[serviceId]['scope'] = scope.split('=')[1]
                serviceList[serviceId]['sortId'] = serviceList[serviceId]['hostname'] + '-' + serviceList[serviceId]['type'] + '-' + serviceList[serviceId]['site']

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as e:
            logger.error(module_class_name(self) + 'Customer:%s Job:%s : Error parsing feed %s - %s' % (logger.customer, logger.job, self._o.scheme + '://' + self._o.netloc + SERVENDPI,
                                                                                                        repr(e).replace('\'', '').replace('\"', '')))
            raise e

    def getServiceEndpoints(self, serviceList, scope):
        try:
            if self.paging:
                count, cursor = 1, 0
                while count != 0:
                    doc = self._get_xmldata(scope, SERVENDPI + '&next_cursor=' + str(cursor))
                    count = int(doc.getElementsByTagName('count')[0].childNodes[0].data)
                    links = doc.getElementsByTagName('link')
                    for le in links:
                        if le.getAttribute('rel') == 'next':
                            href = le.getAttribute('href')
                            for e in href.split('&'):
                                if 'next_cursor' in e:
                                    cursor = e.split('=')[1]
                    self._get_service_endpoints(serviceList, scope, doc)

            else:
                doc = self._get_xmldata(scope, SERVENDPI)
                self._get_service_endpoints(serviceList, scope, doc)

        except input.ConnectorError as e:
            raise e

        except Exception as e:
            raise e

    def _get_sites_internal(self, siteList, scope, doc):
        try:
            sites = doc.getElementsByTagName('SITE')
            for site in sites:
                siteName = site.getAttribute('NAME')
                if siteName not in siteList:
                    siteList[siteName] = {'site': siteName}
                siteList[siteName]['infrastructure'] = getText(site.getElementsByTagName('PRODUCTION_INFRASTRUCTURE')[0].childNodes)
                siteList[siteName]['certification'] = getText(site.getElementsByTagName('CERTIFICATION_STATUS')[0].childNodes)
                siteList[siteName]['ngi'] = getText(site.getElementsByTagName('ROC')[0].childNodes)
                siteList[siteName]['scope'] = scope.split('=')[1]

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as e:
            logger.error(module_class_name(self) + 'Customer:%s Job:%s : Error parsing feed %s - %s' % (logger.customer, logger.job, self._o.scheme + '://' + self._o.netloc + SITESPI,
                                                                                                        repr(e).replace('\'', '').replace('\"', '')))
            raise e

    def getSitesInternal(self, siteList, scope):
        try:
            if self.paging:
                count, cursor = 1, 0
                while count != 0:
                    doc = self._get_xmldata(scope, SITESPI + '&next_cursor=' + str(cursor))
                    count = int(doc.getElementsByTagName('count')[0].childNodes[0].data)
                    links = doc.getElementsByTagName('link')
                    for le in links:
                        if le.getAttribute('rel') == 'next':
                            href = le.getAttribute('href')
                            for e in href.split('&'):
                                if 'next_cursor' in e:
                                    cursor = e.split('=')[1]
                    self._get_sites_internal(siteList, scope, doc)

            else:
                doc = self._get_xmldata(scope, SITESPI)
                self._get_sites_internal(siteList, scope, doc)

        except input.ConnectorError as e:
            raise e

        except Exception as e:
            raise e

    def _get_service_groups(self, groupList, scope, doc):
        try:
            doc = self._get_xmldata(scope, SERVGROUPPI)
            groups = doc.getElementsByTagName('SERVICE_GROUP')
            for group in groups:
                groupId = group.getAttribute('PRIMARY_KEY')
                if groupId not in groupList:
                    groupList[groupId] = {}
                groupList[groupId]['name'] = getText(group.getElementsByTagName('NAME')[0].childNodes)
                groupList[groupId]['monitored'] = getText(group.getElementsByTagName('MONITORED')[0].childNodes)
                groupList[groupId]['scope'] = scope.split('=')[1]
                groupList[groupId]['services'] = []
                services = group.getElementsByTagName('SERVICE_ENDPOINT')
                for service in services:
                    serviceDict = {}
                    serviceDict['hostname'] = getText(service.getElementsByTagName('HOSTNAME')[0].childNodes)
                    try:
                        serviceDict['service_id'] = getText(service.getElementsByTagName('PRIMARY_KEY')[0].childNodes)
                    except IndexError:
                        serviceDict['service_id'] = service.getAttribute('PRIMARY_KEY')
                    serviceDict['type'] = getText(service.getElementsByTagName('SERVICE_TYPE')[0].childNodes)
                    serviceDict['monitored'] = getText(service.getElementsByTagName('NODE_MONITORED')[0].childNodes)
                    serviceDict['production'] = getText(service.getElementsByTagName('IN_PRODUCTION')[0].childNodes)
                    groupList[groupId]['services'].append(serviceDict)

        except (KeyError, IndexError, TypeError, AttributeError, AssertionError) as e:
            logger.error(module_class_name(self) + 'Customer:%s Job:%s : Error parsing feed %s - %s' % (logger.customer, logger.job, self._o.scheme + '://' + self._o.netloc + SERVGROUPPI,
                                                                                                        repr(e).replace('\'', '').replace('\"', '')))
            raise e

    def getServiceGroups(self, groupList, scope):
        try:
            if self.paging:
                count, cursor = 1, 0
                while count != 0:
                    doc = self._get_xmldata(scope, SERVGROUPPI + '&next_cursor=' + str(cursor))
                    count = int(doc.getElementsByTagName('count')[0].childNodes[0].data)
                    links = doc.getElementsByTagName('link')
                    for le in links:
                        if le.getAttribute('rel') == 'next':
                            href = le.getAttribute('href')
                            for e in href.split('&'):
                                if 'next_cursor' in e:
                                    cursor = e.split('=')[1]
                    self._get_service_groups(groupList, scope, doc)

            else:
                doc = self._get_xmldata(scope, SERVGROUPPI)
                self._get_service_groups(groupList, scope, doc)

        except input.ConnectorError as e:
            raise e

        except Exception as e:
            raise e


class TopoFilter(object):
    def __init__(self, gg, ge, ggfilter, gefilter):
        self.gg = gg
        self.ge = ge
        self.ggfilter = copy.copy(ggfilter)
        self.gefilter = copy.copy(gefilter)
        self.subgroupfilter = self.extract_filter('site', self.ggfilter) or \
            self.extract_filter('servicegroup', self.ggfilter)
        self.groupfilter = self.extract_filter('ngi', self.ggfilter)
        self.topofilter()

    def topofilter(self):
        if self.subgroupfilter:
            self.gg = list(filter(lambda e: e['subgroup'].lower() in self.subgroupfilter, self.gg))

        if self.groupfilter:
            self.gg = list(filter(lambda e: e['group'].lower() in self.groupfilter, self.gg))

        if self.ggfilter:
            self.gg = self.filter_tags(self.ggfilter, self.gg)

        allsubgroups = set([e['subgroup'] for e in self.gg])
        if allsubgroups:
            self.ge = list(filter(lambda e: e['group'] in allsubgroups, self.ge))

        if self.gefilter:
            self.ge = self.filter_tags(self.gefilter, self.ge)

    def extract_filter(self, tag, ggtags):
        gg = None
        if tag.lower() in [t.lower() for t in ggtags.keys()]:
            for k, v in ggtags.items():
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
                if value == '1':
                    value = 'Y'
                elif value == '0':
                    value = 'N'
                if isinstance(tags[attr], list):
                    for a in tags[attr]:
                        if value.lower() == a.lower():
                            return True
                else:
                    if value.lower() == tags[attr].lower():
                        return True
            try:
                listofelem = list(filter(getit, listofelem))
            except KeyError as e:
                logger.error('Customer:%s Job:%s : Wrong tags specified: %s' % (logger.customer, logger.job, e))
        return listofelem


def main():
    global logger, globopts, confcust
    parser = argparse.ArgumentParser(description="""Fetch entities (ServiceGroups, Sites, Endpoints)
                                                    from GOCDB for every customer and job listed in customer.conf and write them
                                                    in an appropriate place""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    args = parser.parse_args()
    group_endpoints, group_groups = [], []
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
    feeds = confcust.get_mapfeedjobs(sys.argv[0], 'GOCDB', deffeed='https://goc.egi.eu/gocdbpi/')

    for feed, jobcust in feeds.items():
        scopes = confcust.get_feedscopes(feed, jobcust)
        paging = confcust.is_paginated(feed, jobcust)
        auth_custopts = confcust.get_authopts(feed, jobcust)
        auth_opts = cglob.merge_opts(auth_custopts, 'authentication')
        auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
        if auth_complete:
            gocdb = GOCDBReader(feed, scopes, paging, auth=auth_opts)
        else:
            logger.error('%s options incomplete, missing %s' % ('authentication', ' '.join(missing)))
            continue

        for job, cust in jobcust:
            jobdir = confcust.get_fulldir(cust, job)
            jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)

            global fetchtype, custname
            fetchtype = confcust.get_fetchtype(job)
            uidservtype = confcust.pass_uidserviceendpoints(job)
            custname = confcust.get_custname(cust)

            logger.customer = custname
            logger.job = job

            ams_custopts = confcust.get_amsopts(cust)
            ams_opts = cglob.merge_opts(ams_custopts, 'ams')
            ams_complete, missopt = cglob.is_complete(ams_opts, 'ams')
            if not ams_complete:
                logger.error('Customer:%s Job:%s %s options incomplete, missing %s' % (custname, logger.job, 'ams', ' '.join(missopt)))
                continue

            if fetchtype == 'ServiceGroups':
                group_endpoints = gocdb.getGroupOfServices(uidservtype)
            else:
                group_endpoints = gocdb.getGroupOfEndpoints(uidservtype)
            group_groups = gocdb.getGroupOfGroups()

            if fixed_date:
                output.write_state(sys.argv[0], jobstatedir, gocdb.state,
                                   globopts['InputStateDays'.lower()],
                                   fixed_date.replace('-', '_'))
            else:
                output.write_state(sys.argv[0], jobstatedir, gocdb.state,
                                   globopts['InputStateDays'.lower()])

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
                                        int(globopts['ConnectionTimeout'.lower()]),
                                        int(globopts['ConnectionSleepRetry'.lower()]))

                ams.send(globopts['AvroSchemasTopologyGroupOfGroups'.lower()],
                         'group_groups', partdate, group_groups)

                ams.send(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()],
                         'group_endpoints', partdate, group_endpoints)

            if eval(globopts['GeneralWriteAvro'.lower()]):
                if fixed_date:
                    filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], jobdir, fixed_date.replace('-', '_'))
                else:
                    filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
                ret, excep = avro.write(group_groups)
                if not ret:
                    logger.error('Customer:%s Job:%s : %s' % (logger.customer, logger.job, repr(excep)))
                    raise SystemExit(1)

                if fixed_date:
                    filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], jobdir, fixed_date.replace('-', '_'))
                else:
                    filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename)
                ret, excep = avro.write(group_endpoints)
                if not ret:
                    logger.error('Customer:%s Job:%s : %s' % (logger.customer, logger.job, repr(excep)))
                    raise SystemExit(1)

            logger.info('Customer:' + custname + ' Job:' + job + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (fetchtype, numgg))
            if getags or ggtags:
                selstr = 'Customer:%s Job:%s Selected ' % (custname, job)
                selge, selgg = '', ''
                if getags:
                    for key, value in getags.items():
                        if isinstance(value, list):
                            value = '[' + ','.join(value) + ']'
                        selge += '%s:%s,' % (key, value)
                    selstr += 'Endpoints(%s):' % selge[:len(selge) - 1]
                    selstr += '%d ' % (len(group_endpoints))
                if ggtags:
                    for key, value in ggtags.items():
                        if isinstance(value, list):
                            value = '[' + ','.join(value) + ']'
                        selgg += '%s:%s,' % (key, value)
                    selstr += 'Groups(%s):' % selgg[:len(selgg) - 1]
                    selstr += '%d' % (len(group_groups))

                logger.info(selstr)


if __name__ == '__main__':
    main()
