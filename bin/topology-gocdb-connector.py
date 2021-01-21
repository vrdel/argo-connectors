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
import xml.dom.minidom

from argo_egi_connectors import input
from argo_egi_connectors import output
from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.gocdb_topology import GOCDBParseServiceGroups, GOCDBParseServiceEndpoints

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.helpers import filename_date, module_class_name, datestamp, date_check
from urllib.parse import urlparse

logger = None

# GOCDB explicitly says &scope='' for all scopes
SERVENDPI = '/gocdbpi/private/?method=get_service_endpoint&scope='
SITESPI = '/gocdbpi/private/?method=get_site&scope='
SERVGROUPPI = '/gocdbpi/private/?method=get_service_group&scope='

globopts = {}
custname = ''

isok = True


def parse_source_servicegroups(res, custname, uidservtype):
    group_groups = GOCDBParseServiceGroups(logger, res, custname, uidservtype).get_group_groups()
    group_endpoints = GOCDBParseServiceGroups(logger, res, custname, uidservtype).get_group_endpoints()

    return group_groups, group_endpoints


def parse_source_endpoints(res, custname, uidservtype):
    group_endpoints = GOCDBParseServiceEndpoints(logger, res, custname, uidservtype).get_group_endpoints()

    return group_endpoints


def find_paging_cursor_count(res):
    cursor, count = 1, 0

    doc = xml.dom.minidom.parseString(res)
    count = int(doc.getElementsByTagName('count')[0].childNodes[0].data)
    links = doc.getElementsByTagName('link')
    for le in links:
        if le.getAttribute('rel') == 'next':
            href = le.getAttribute('href')
            for e in href.split('&'):
                if 'next_cursor' in e:
                    cursor = e.split('=')[1]

    return count, cursor


def fetch_data(feed, api, auth_opts):
    feed_parts = urlparse(feed)
    res = None

    res = input.connection(logger, os.path.basename(sys.argv[0]), globopts,
                            feed_parts.scheme, feed_parts.netloc, api,
                            custauth=auth_opts)

    return res


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
    topofeed = confcust.get_topofeed()
    topofeedpaging = confcust.get_topofeedpaging()
    uidservtype = confcust.get_uidserviceendpoints()
    topofetchtype = confcust.get_topofetchtype()
    custname = confcust.get_custname()
    logger.customer = custname

    auth_custopts = confcust.get_authopts()
    auth_opts = cglob.merge_opts(auth_custopts, 'authentication')
    auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
    if not auth_complete:
        logger.error('%s options incomplete, missing %s' % ('authentication', ' '.join(missing)))
        raise SystemExit(1)

    try:
        group_endpoints, group_groups = list(), list()

        if topofeedpaging:
            count, cursor = 1, 0
            while count != 0:
                res = fetch_data(topofeed, f'{SERVGROUPPI}&next_cursor={str(cursor)}', auth_opts)
                count, cursor = find_paging_cursor_count(res)
                tmp_gg, tmp_ge = parse_source_servicegroups(res, custname, uidservtype)
                group_endpoints += tmp_ge
                group_groups += tmp_gg
        else:
            res = fetch_data(topofeed, SERVGROUPPI, auth_opts)
            group_groups, group_endpoints = parse_source_servicegroups(res, custname, uidservtype)

        if topofeedpaging:
            count, cursor = 1, 0
            while count != 0:
                res = fetch_data(topofeed, f'{SERVENDPI}&next_cursor={str(cursor)}', auth_opts)
                count, cursor = find_paging_cursor_count(res)
                tmp_ge = parse_source_endpoints(res, custname, uidservtype)
                group_endpoints += tmp_ge
        else:
            res = fetch_data(topofeed, SERVGROUPPI, auth_opts)
            group_endpoints += parse_source_endpoints(res, custname, uidservtype)

        # safely assume here one customer defined in customer file
        cust = list(confcust.get_customers())[0]
        statedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
        if fixed_date:
            output.write_state(sys.argv[0], statedir, gocdb.state,
                               globopts['InputStateDays'.lower()],
                               fixed_date.replace('-', '_'))
        else:
            output.write_state(sys.argv[0], statedir, gocdb.state,
                               globopts['InputStateDays'.lower()])

        if not gocdb.state:
            raise SystemExit(1)

        webapi_custopts = confcust.get_webapiopts()
        webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
        webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
        if not webapi_complete:
            logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
            raise SystemExit(1)

        group_endpoints = gocdb.get_group_endpoints()
        group_groups = gocdb.get_group_groups()

        numge = len(group_endpoints)
        numgg = len(group_groups)

        if eval(globopts['GeneralPublishWebAPI'.lower()]):
            webapi = output.WebAPI(sys.argv[0], webapi_opts['webapihost'],
                                webapi_opts['webapitoken'], logger,
                                int(globopts['ConnectionRetry'.lower()]),
                                int(globopts['ConnectionTimeout'.lower()]),
                                int(globopts['ConnectionSleepRetry'.lower()]),
                                verifycert=globopts['AuthenticationVerifyServerCert'.lower()])
            webapi.send(group_groups, 'groups')
            webapi.send(group_endpoints, 'endpoints')

        custdir = confcust.get_custdir()
        if eval(globopts['GeneralWriteAvro'.lower()]):
            if fixed_date:
                filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir, fixed_date.replace('-', '_'))
            else:
                filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir)
            avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
            ret, excep = avro.write(group_groups)
            if not ret:
                logger.error('Customer:%s Job:%s : %s' % (logger.customer, logger.job, repr(excep)))
                raise SystemExit(1)

            if fixed_date:
                filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir, fixed_date.replace('-', '_'))
            else:
                filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir)
            avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename)
            ret, excep = avro.write(group_endpoints)
            if not ret:
                logger.error('Customer:%s: %s' % (logger.customer, repr(excep)))
                raise SystemExit(1)

        logger.info('Customer:' + custname + ' Type:%s ' % (','.join(topofetchtype)) + 'Fetched Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))

    except input.ConnectorError:
        pass


if __name__ == '__main__':
    main()
