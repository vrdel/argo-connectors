#!/usr/bin/python3

import argparse
import os
import sys
import json

import uvloop
import asyncio

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.log import Logger
from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.utils import filename_date, datestamp, date_check
from argo_egi_connectors.parse.eoscprovider_topology import ParseTopo
from argo_egi_connectors.parse.eoscprovider_contacts import ParseResourcesContacts
from argo_egi_connectors.mesh.contacts import attach_contacts_topodata

from urllib.parse import urlparse

logger = None
globopts = {}
custname = ''


def parse_source(resources, providers, uidservendp, custname):
    topo = ParseTopo(logger, providers, resources, uidservendp, custname)

    return topo.get_group_groups(), topo.get_group_endpoints()


async def send_webapi(webapi_opts, data, topotype, fixed_date=None):
    webapi = WebAPI(sys.argv[0], webapi_opts['webapihost'],
                    webapi_opts['webapitoken'], logger,
                    int(globopts['ConnectionRetry'.lower()]),
                    int(globopts['ConnectionTimeout'.lower()]),
                    int(globopts['ConnectionSleepRetry'.lower()]),
                    date=fixed_date)
    await webapi.send(data, topotype)


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


def find_next_paging_cursor_count(res):
    cursor, count = None, None

    doc = json.loads(res)
    total = doc['total']
    from_index = doc['from']
    to_index = doc['to']

    return total, from_index, to_index


def filter_out_results(data):
    json_data = json.loads(data)['results']
    return json_data


async def fetch_data(feed, paginated):
    fetched_data = list()
    remote_topo = urlparse(feed)
    session = SessionWithRetry(logger, custname, globopts, handle_session_close=True)

    res = await session.http_get('{}://{}{}'.format(remote_topo.scheme,
                                                    remote_topo.netloc,
                                                    remote_topo.path))
    if paginated:
        fetched_results = filter_out_results(res)
        total, from_index, to_index = find_next_paging_cursor_count(res)
        num = to_index - from_index
        from_index = to_index

        while to_index != total:
            res = await \
                session.http_get('{}://{}{}?from={}&quantity={}'.format(remote_topo.scheme,
                                                                        remote_topo.netloc,
                                                                        remote_topo.path,
                                                                        from_index,
                                                                        num))
            fetched_results = fetched_results + filter_out_results(res)

            total, from_index, to_index = find_next_paging_cursor_count(res)
            num = to_index - from_index
            from_index = to_index

        await session.close()
        return dict(results=fetched_results)

    else:
        total, from_index, to_index = find_next_paging_cursor_count(res)
        num = total
        from_index = 0

        res = await \
            session.http_get('{}://{}{}?from={}&quantity={}'.format(remote_topo.scheme,
                                                                    remote_topo.netloc,
                                                                    remote_topo.path,
                                                                    from_index,
                                                                    num))
        fetched_data.append(res)

        await session.close()
        return fetched_data


async def write_state(confcust, fixed_date, state):
    cust = list(confcust.get_customers())[0]
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    fetchtype = confcust.get_topofetchtype()
    if fixed_date:
        await state_write(sys.argv[0], jobstatedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(sys.argv[0], jobstatedir, state,
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

    parser = argparse.ArgumentParser(description="""Fetch and construct entities from EOSC-PROVIDER feed""")
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
    global custname
    custname = confcust.get_custname()

    # safely assume here one customer defined in customer file
    cust = list(confcust.get_customers())[0]
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    fetchtype = confcust.get_topofetchtype()[0]

    state = None
    logger.customer = custname
    uidservendp = confcust.get_uidserviceendpoints()
    topofeed = confcust.get_topofeed()
    topofeedpaging = confcust.get_topofeedpaging()

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        topofeedproviders = confcust.get_topofeedsites()
        topofeedresources = confcust.get_topofeedendpoints()
        coros = [
            fetch_data(topofeedresources, topofeedpaging), fetch_data(topofeedproviders, topofeedpaging)
        ]

        # fetch topology data concurrently in coroutines
        fetched_resources, fetched_providers = loop.run_until_complete(asyncio.gather(*coros, return_exceptions=True))

        group_groups, group_endpoints = parse_source(fetched_resources, fetched_providers, uidservendp, custname)
        endpoints_contacts = ParseResourcesContacts(logger, fetched_resources).get_contacts()

        attach_contacts_topodata(logger, endpoints_contacts, group_endpoints)

        loop.run_until_complete(
            write_state(confcust, fixed_date, True)
        )

        webapi_opts = get_webapi_opts(cglob, confcust)

        numge = len(group_endpoints)
        numgg = len(group_groups)

        # send concurrently to WEB-API in coroutines
        if eval(globopts['GeneralPublishWebAPI'.lower()]):
            loop.run_until_complete(
                asyncio.gather(
                    send_webapi(webapi_opts, group_groups, 'groups', fixed_date),
                    send_webapi(webapi_opts, group_endpoints,'endpoints', fixed_date)
                )
            )

        if eval(globopts['GeneralWriteAvro'.lower()]):
            write_avro(confcust, group_groups, group_endpoints, fixed_date)

        logger.info('Customer:' + custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (fetchtype, numgg))

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(confcust, fixed_date, False )
        )


if __name__ == '__main__':
    main()
