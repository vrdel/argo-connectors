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

import uvloop
import asyncio
from concurrent.futures import ProcessPoolExecutor
from functools import partial

from argo_egi_connectors.io.connection import ConnectorError, SessionWithRetry
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.log import Logger
from argo_egi_connectors.parse.csvtopo import ParseServiceGroupsEndpoints

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.tools import filename_date, module_class_name, datestamp, date_check
from urllib.parse import urlparse

import csv
import json
from io import StringIO

logger = None

globopts = {}
custname = ''

isok = True


def parse_source_csvtopo(res, custname, uidservtype):
    topo_csv = ParseServiceGroupsEndpoints(logger, res, custname, uidservtype)
    group_groups = topo_csv.get_groupgroups()
    group_endpoints = topo_csv.get_groupendpoints()
    return group_groups, group_endpoints


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


async def write_state(confcust, fixed_date, state):
    # safely assume here one customer defined in customer file
    cust = list(confcust.get_customers())[0]
    statedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    if fixed_date:
        await state_write(sys.argv[0], statedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(sys.argv[0], statedir, state,
                          globopts['InputStateDays'.lower()])


async def fetch_data(feed, auth_opts):
    feed_parts = urlparse(feed)
    session = SessionWithRetry(logger, os.path.basename(sys.argv[0]),
                                globopts, custauth=auth_opts)
    res = await session.http_get('{}://{}{}?{}'.format(feed_parts.scheme,
                                                       feed_parts.netloc,
                                                       feed_parts.path,
                                                       feed_parts.query))
    return res


def csv_to_json(csvdata):
    data = StringIO(csvdata)
    reader = csv.reader(data, delimiter=',')

    num_row = 0
    results = []
    header = []
    for row in reader:
        if num_row == 0:
            header = row
            num_row = num_row + 1
            continue
        num_item = 0
        datum = {}
        for item in header:
            datum[item] = row[num_item]
            num_item = num_item + 1
        results.append(datum)

    return results

async def send_webapi(webapi_opts, data, topotype):
    webapi = WebAPI(sys.argv[0], webapi_opts['webapihost'],
                    webapi_opts['webapitoken'], logger,
                    int(globopts['ConnectionRetry'.lower()]),
                    int(globopts['ConnectionTimeout'.lower()]),
                    int(globopts['ConnectionSleepRetry'.lower()]))
    await webapi.send(data, topotype)


def write_avro(confcust, group_groups, group_endpoints, fixed_date):
    custdir = confcust.get_custdir()
    if fixed_date:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir)
    avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
    ret, excep = avro.write(group_groups)
    if not ret:
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)

    if fixed_date:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir)
    avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename)
    ret, excep = avro.write(group_endpoints)
    if not ret:
        logger.error('Customer:%s: %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)


def main():
    global logger, globopts, confcust
    parser = argparse.ArgumentParser(description="""Fetch entities (ServiceGroups, Sites, Endpoints)
                                                    from CSV topology feed for every customer and job listed in customer.conf and write them
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

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        group_endpoints, group_groups = list(), list()

        # fetch topology data concurrently in coroutines
        fetched_topology = loop.run_until_complete(fetch_data(topofeed, auth_opts))

        try:
            topo_json = csv_to_json(fetched_topology)

            group_groups, group_endpoints = parse_source_csvtopo(topo_json,
                                                                custname,
                                                                uidservtype)
        except Exception as exc:
            raise ConnectorError

        loop.run_until_complete(
            write_state(confcust, fixed_date, True)
        )

        webapi_opts = get_webapi_opts(cglob, confcust)

        numgg = len(group_groups)
        numge = len(group_endpoints)

        # send concurrently to WEB-API in coroutines
        if eval(globopts['GeneralPublishWebAPI'.lower()]):
            loop.run_until_complete(
                asyncio.gather(
                    send_webapi(webapi_opts, group_groups, 'groups'),
                    send_webapi(webapi_opts, group_endpoints,'endpoints')
                )
            )

        if eval(globopts['GeneralWriteAvro'.lower()]):
            write_avro(confcust, group_groups, group_endpoints, fixed_date)

        logger.info('Customer:' + custname + ' Type:%s ' % (','.join(topofetchtype)) + 'Fetched Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))

    except ConnectorError:
        loop.run_until_complete(
            write_state(confcust, fixed_date, False )
        )

    finally:
        loop.close()


if __name__ == '__main__':
    main()