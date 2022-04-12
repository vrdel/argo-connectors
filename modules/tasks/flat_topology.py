import json
import asyncio

from urllib.parse import urlparse

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.parse.flat_topology import ParseFlatEndpoints
from argo_egi_connectors.parse.flat_contacts import ParseContacts
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.mesh.contacts import attach_contacts_topodata
from argo_egi_connectors.tasks.common import write_state, write_avro


def is_feed(feed):
    data = urlparse(feed)

    if not data.netloc:
        return False
    else:
        return True


async def fetch_data(logger, custname, globopts, feed):
    remote_topo = urlparse(feed)
    session = SessionWithRetry(logger, custname, globopts)
    if remote_topo.query:
        res = await session.http_get('{}://{}{}?{}'.format(remote_topo.scheme,
                                                        remote_topo.netloc,
                                                        remote_topo.path,
                                                        remote_topo.query))
    else:
        res = await session.http_get('{}://{}{}'.format(remote_topo.scheme,
                                                        remote_topo.netloc,
                                                        remote_topo.path))
    return res


def parse_source_topo(logger, custname, res, uidservendp, fetchtype, is_csv=False):
    # group_groups, group_endpoints = ParseEoscTopo(logger, res, uidservtype, fetchtype).get_data()
    topo = ParseFlatEndpoints(logger, res, custname, uidservendp, fetchtype, is_csv, scope=custname)
    group_groups = topo.get_groupgroups()
    group_endpoints = topo.get_groupendpoints()

    return group_groups, group_endpoints


async def send_webapi(logger, connector_name, globopts, webapi_opts, data, topotype, fixed_date=None):
    webapi = WebAPI(connector_name, webapi_opts['webapihost'],
                    webapi_opts['webapitoken'], logger,
                    int(globopts['ConnectionRetry'.lower()]),
                    int(globopts['ConnectionTimeout'.lower()]),
                    int(globopts['ConnectionSleepRetry'.lower()]),
                    date=fixed_date)
    await webapi.send(data, topotype)


async def run(loop, logger, connector_name, globopts, webapi_opts, confcust,
              custname, topofeed, fetchtype, fixed_date, uidservendp, is_csv=False):
    if is_feed(topofeed):
        res = await(fetch_data(logger, custname, globopts, topofeed))
        group_groups, group_endpoints = parse_source_topo(logger, custname, res, uidservendp, fetchtype, is_csv)
        contacts = ParseContacts(logger, res, uidservendp, is_csv).get_contacts()
        attach_contacts_topodata(logger, contacts, group_endpoints)

    elif not is_feed(topofeed) and not is_csv:
        try:
            with open(topofeed) as fp:
                js = json.load(fp)
                group_groups, group_endpoints = parse_source_topo(logger, custname, js, uidservendp, fetchtype)
        except IOError as exc:
            logger.error('Customer:%s : Problem opening %s - %s' % (logger.customer, topofeed, repr(exc)))

    await write_state(connector_name, globopts, confcust, fixed_date, True)

    numge = len(group_endpoints)
    numgg = len(group_groups)

    # send concurrently to WEB-API in coroutines
    if eval(globopts['GeneralPublishWebAPI'.lower()]):
        task_groups = loop.create_task(send_webapi(logger, connector_name, globopts, webapi_opts, group_groups, 'groups', fixed_date))
        task_endpoints = loop.create_task(send_webapi(logger, connector_name, globopts, webapi_opts, group_endpoints,'endpoints', fixed_date))
        await task_groups
        await task_endpoints

    if eval(globopts['GeneralWriteAvro'.lower()]):
        write_avro(logger, globopts, confcust, group_groups, group_endpoints, fixed_date)

    logger.info('Customer:' + custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (fetchtype, numgg))
