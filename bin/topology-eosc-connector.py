#!/usr/bin/python3

import argparse
import os
import sys
import json

from argo_egi_connectors.io.connection import ConnectionWithRetry, ConnectorError
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.log import Logger
from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.tools import filename_date, datestamp, date_check
from argo_egi_connectors.parse.eosc import ParseEoscTopo

from urllib.parse import urlparse

logger = None
globopts = {}


def is_feed(feed):
    data = urlparse(feed)

    if not data.netloc:
        return False
    else:
        return True


def fetch_data(feed):
    remote_topo = urlparse(feed)
    res = ConnectionWithRetry(logger, 'EOSC', globopts, remote_topo.scheme, remote_topo.netloc, remote_topo.path)
    return res


def parse_source(res, uidservtype, fetchtype):
    group_groups, group_endpoints = ParseEoscTopo(logger, res, uidservtype, fetchtype).get_data()
    return group_groups, group_endpoints


def write_state(confcust, fixed_date, state):
    cust = list(confcust.get_customers())[0]
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    fetchtype = confcust.get_topofetchtype()
    if fixed_date:
        state_write(sys.argv[0], jobstatedir, state,
                    globopts['InputStateDays'.lower()],
                    fixed_date.replace('-', '_'))
    else:
        state_write(sys.argv[0], jobstatedir, state,
                    globopts['InputStateDays'.lower()])


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
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)


def main():
    global logger, globopts, confcust

    parser = argparse.ArgumentParser(description="""Fetch and construct entities from EOSC-PORTAL feed""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    args = parser.parse_args()
    group_endpoints, group_groups = list(), list()
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
    custname = confcust.get_custname()

    # safely assume here one customer defined in customer file
    cust = list(confcust.get_customers())[0]
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    fetchtype = confcust.get_topofetchtype()[0]

    state = None
    logger.customer = custname
    uidservtype = confcust.get_uidserviceendpoints()
    topofeed = confcust.get_topofeed()

    try:
        if is_feed(topofeed):
            res = fetch_data(topofeed)
            group_groups, group_endpoints = parse_source(res, uidservtype, fetchtype)
        else:
            try:
                with open(topofeed) as fp:
                    js = json.load(fp)
                    group_groups, group_endpoints = parse_source(js, uidservtype, fetchtype)
            except IOError as exc:
                logger.error('Customer:%s : Problem opening %s - %s' % (logger.customer, topofeed, repr(exc)))

        write_state(confcust, fixed_date, True)

        numge = len(group_endpoints)
        numgg = len(group_groups)

        if eval(globopts['GeneralWriteAvro'.lower()]):
            write_avro(confcust, group_groups, group_endpoints, fixed_date)

        logger.info('Customer:' + custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (fetchtype, numgg))

    except ConnectorError:
        write_state(confcust, fixed_date, False)


if __name__ == '__main__':
    main()
