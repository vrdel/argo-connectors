import os
import asyncio
import xml.dom.minidom

from concurrent.futures import ProcessPoolExecutor
from functools import partial

from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts, ParseServiceEndpointContacts, ParseServiceGroupRoles, ParseSitesWithContacts, ParseServiceGroupWithContacts

from argo_egi_connectors.exceptions import ConnectorParseError, ConnectorHttpError
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.io.ldap import LDAPSessionWithRetry
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.mesh.contacts import attach_contacts_topodata
from argo_egi_connectors.mesh.srm_port import attach_srmport_topodata
from argo_egi_connectors.mesh.storage_element_path import attach_sepath_topodata
from argo_egi_connectors.tasks.common import write_state, write_avro

from urllib.parse import urlparse


def filter_multiple_tags(data):
    """
        Paginated content is represented with multiple XML enclosing tags
        in a single buffer:

        <?xml version="1.0" encoding="UTF-8"?>
        <results>
            topology entities
        </results>
        <?xml version="1.0" encoding="UTF-8"?>
        <results>
            topology entities
        </results>
        ...

        Remove them and leave only one enclosing.
    """
    data_lines = data.split('\n')
    data_lines = list(filter(lambda line:
                             '</results>' not in line
                             and '<results>' not in line
                             and '<?xml' not in line,
                             data_lines))
    data_lines.insert(0, '<?xml version="1.0" encoding="UTF-8"?>')
    data_lines.insert(1, '<results>')
    data_lines.append('</results>')
    return '\n'.join(data_lines)


def find_next_paging_cursor_count(res):
    cursor, count = None, None

    doc = xml.dom.minidom.parseString(res)
    count = int(doc.getElementsByTagName('count')[0].childNodes[0].data)
    links = doc.getElementsByTagName('link')
    for link in links:
        if link.getAttribute('rel') == 'next':
            href = link.getAttribute('href')
            for query in href.split('&'):
                if 'next_cursor' in query:
                    cursor = query.split('=')[1]

    return count, cursor


def parse_source_servicegroups(logger, res, custname, uidservendp, pass_extensions):
    group_groups = ParseServiceGroups(logger, res, custname, uidservendp,
                                      pass_extensions).get_group_groups()
    group_endpoints = ParseServiceGroups(logger, res, custname, uidservendp,
                                         pass_extensions).get_group_endpoints()

    return group_groups, group_endpoints


def parse_source_endpoints(logger, res, custname, uidservendp, pass_extensions):
    group_endpoints = ParseServiceEndpoints(logger, res, custname, uidservendp,
                                            pass_extensions).get_group_endpoints()

    return group_endpoints


def parse_source_sites(logger, res, custname, uidservendp, pass_extensions):
    group_groups = ParseSites(logger, res, custname, uidservendp,
                              pass_extensions).get_group_groups()

    return group_groups


def parse_source_sitescontacts(logger, res, custname):
    contacts = ParseSiteContacts(logger, res)
    return contacts.get_contacts()


def parse_source_siteswithcontacts(logger, res, custname):
    contacts = ParseSitesWithContacts(logger, res)
    return contacts.get_contacts()


def parse_source_servicegroupscontacts(logger, res, custname):
    contacts = ParseServiceGroupWithContacts(logger, res)
    return contacts.get_contacts()


def parse_source_servicegroupsroles(logger, res, custname):
    contacts = ParseServiceGroupRoles(logger, res)
    return contacts.get_contacts()


def parse_source_serviceendpoints_contacts(logger, res, custname):
    contacts = ParseServiceEndpointContacts(logger, res)
    return contacts.get_contacts()


# Fetches data from LDAP, connection parameters are set in customer.conf
async def fetch_ldap_data(logger, globopts, host, port, base, filter, attributes):
    ldap_session = LDAPSessionWithRetry(logger, int(globopts['ConnectionRetry'.lower()]),
        int(globopts['ConnectionSleepRetry'.lower()]), int(globopts['ConnectionTimeout'.lower()]))

    res = await ldap_session.search(host, port, base, filter, attributes)
    return res


async def fetch_data(logger, connector_name, globopts, api, auth_opts,
                     paginated):
    feed_parts = urlparse(api)
    fetched_data = list()
    if paginated:
        count, cursor = 1, 0
        while count != 0:
            session = SessionWithRetry(logger, os.path.basename(connector_name),
                                       globopts, custauth=auth_opts)
            res = await session.http_get('{}&next_cursor={}'.format(api,
                                                                    cursor))
            count, cursor = find_next_paging_cursor_count(res)
            fetched_data.append(res)
        return filter_multiple_tags(''.join(fetched_data))

    else:
        session = SessionWithRetry(logger, os.path.basename(connector_name),
                                   globopts, custauth=auth_opts)
        res = await session.http_get(api)
        return res


async def send_webapi(logger, connector_name, globopts, webapi_opts, data, topotype, fixed_date=None):
    webapi = WebAPI(connector_name, webapi_opts['webapihost'],
                    webapi_opts['webapitoken'], logger,
                    int(globopts['ConnectionRetry'.lower()]),
                    int(globopts['ConnectionTimeout'.lower()]),
                    int(globopts['ConnectionSleepRetry'.lower()]),
                    date=fixed_date)
    await webapi.send(data, topotype)


def contains_exception(list):
    for a in list:
        if isinstance(a, Exception):
            return (True, a)

    return (False, None)


async def run(loop, logger, connector_name, SITE_CONTACTS,
              SERVICEGROUP_CONTACTS, SERVICE_ENDPOINTS_PI, SERVICE_GROUPS_PI,
              SITES_PI, globopts, auth_opts, webapi_opts,
              bdii_opts, confcust, custname, topofeed, topofetchtype,
              fixed_date, uidservendp, pass_extensions, topofeedpaging):
    fetched_sites, fetched_servicegroups, fetched_endpoints = None, None, None
    fetched_bdii = None

    group_endpoints, group_groups = list(), list()
    parsed_site_contacts, parsed_servicegroups_contacts, parsed_serviceendpoint_contacts = None, None, None

    try:
        contact_coros = [
            fetch_data(logger, connector_name, globopts, topofeed + SITE_CONTACTS, auth_opts, False),
            fetch_data(logger, connector_name, globopts, topofeed + SERVICEGROUP_CONTACTS, auth_opts, False)
        ]
        contacts = await asyncio.gather(*contact_coros, loop=loop, return_exceptions=True)

        exc_raised, exc = contains_exception(contacts)
        if exc_raised:
            raise ConnectorHttpError(repr(exc))

        parsed_site_contacts = parse_source_sitescontacts(logger, contacts[0], custname)
        parsed_servicegroups_contacts = parse_source_servicegroupsroles(logger, contacts[1], custname)

    except (ConnectorHttpError, ConnectorParseError) as exc:
        logger.warn('SITE_CONTACTS and SERVICERGOUP_CONTACT methods not implemented')

    coros = [fetch_data(logger, connector_name, globopts, SERVICE_ENDPOINTS_PI, auth_opts, topofeedpaging)]
    if 'servicegroups' in topofetchtype:
        coros.append(fetch_data(logger, connector_name, globopts, SERVICE_GROUPS_PI, auth_opts, topofeedpaging))
    if 'sites' in topofetchtype:
        coros.append(fetch_data(logger, connector_name, globopts, SITES_PI, auth_opts, topofeedpaging))

    if bdii_opts and eval(bdii_opts['bdii']):
        host = bdii_opts['bdiihost']
        port = bdii_opts['bdiiport']
        base = bdii_opts['bdiiquerybase']

        coros.append(fetch_ldap_data(logger, globopts, host, port, base,
                                     bdii_opts['bdiiqueryfiltersrm'],
                                     bdii_opts['bdiiqueryattributessrm'].split(' ')))

        coros.append(fetch_ldap_data(logger, globopts, host, port, base,
                                     bdii_opts['bdiiqueryfiltersepath'],
                                     bdii_opts['bdiiqueryattributessepath'].split(' ')))

    # fetch topology data concurrently in coroutines
    fetched_topology = await asyncio.gather(*coros, loop=loop, return_exceptions=True)

    fetched_endpoints = fetched_topology[0]
    if bdii_opts and eval(bdii_opts['bdii']):
        fetched_bdii = list()
        fetched_bdii.append(fetched_topology[-2])
        fetched_bdii.append(fetched_topology[-1])
    if 'sites' in topofetchtype and 'servicegroups' in topofetchtype:
        fetched_servicegroups, fetched_sites = (fetched_topology[1], fetched_topology[2])
    elif 'sites' in topofetchtype:
        fetched_sites = fetched_topology[1]
    elif 'servicegroups' in topofetchtype:
        fetched_servicegroups = fetched_topology[1]

    exc_raised, exc = contains_exception(fetched_topology)
    if exc_raised:
        raise ConnectorHttpError(repr(exc))

    # proces data in parallel using multiprocessing
    executor = ProcessPoolExecutor(max_workers=3)
    parse_workers = list()
    exe_parse_source_endpoints = partial(parse_source_endpoints, logger,
                                         fetched_endpoints, custname,
                                         uidservendp, pass_extensions)
    exe_parse_source_servicegroups = partial(parse_source_servicegroups, logger,
                                             fetched_servicegroups, custname,
                                             uidservendp, pass_extensions)
    exe_parse_source_sites = partial(parse_source_sites, logger, fetched_sites,
                                     custname, uidservendp, pass_extensions)

    # parse topology depend on configured components fetch. we can fetch
    # only sites, only servicegroups or both.
    if fetched_servicegroups and fetched_sites:
        parse_workers.append(
            loop.run_in_executor(executor, exe_parse_source_endpoints)
        )
        parse_workers.append(
            loop.run_in_executor(executor, exe_parse_source_servicegroups)
        )
        parse_workers.append(
            loop.run_in_executor(executor, exe_parse_source_sites)
        )
    elif fetched_servicegroups and not fetched_sites:
        parse_workers.append(
            loop.run_in_executor(executor, exe_parse_source_servicegroups)
        )
    elif fetched_sites and not fetched_servicegroups:
        parse_workers.append(
            loop.run_in_executor(executor, exe_parse_source_endpoints)
        )
        parse_workers.append(
            loop.run_in_executor(executor, exe_parse_source_sites)
        )

    parsed_topology = await asyncio.gather(*parse_workers, loop=loop)

    if fetched_servicegroups and fetched_sites:
        group_endpoints = parsed_topology[0]
        group_groups, group_endpoints_sg = parsed_topology[1]
        group_endpoints += group_endpoints_sg
        group_groups += parsed_topology[2]
    elif fetched_servicegroups and not fetched_sites:
        group_groups, group_endpoints = parsed_topology[0]
    elif fetched_sites and not fetched_servicegroups:
        group_endpoints = parsed_topology[0]
        group_groups = parsed_topology[1]


    # check if we fetched SRM port info and attach it appropriate endpoint
    # data
    if bdii_opts and eval(bdii_opts['bdii']):
        attach_srmport_topodata(logger, bdii_opts['bdiiqueryattributessrm'].split(' ')[0], fetched_bdii[0], group_endpoints)
        attach_sepath_topodata(logger, bdii_opts['bdiiqueryattributessepath'].split(' ')[0], fetched_bdii[1], group_endpoints)

    # parse contacts from fetched service endpoints topology, if there are
    # any
    parsed_serviceendpoint_contacts = parse_source_serviceendpoints_contacts(logger, fetched_endpoints, custname)

    if not parsed_site_contacts and fetched_sites:
        # GOCDB has not SITE_CONTACTS, try to grab contacts from fetched
        # sites topology entities
        parsed_site_contacts = parse_source_siteswithcontacts(logger, fetched_sites, custname)

    attach_contacts_workers = [
        loop.run_in_executor(executor, partial(attach_contacts_topodata,
                                                logger,
                                                parsed_site_contacts,
                                                group_groups)),
        loop.run_in_executor(executor, partial(attach_contacts_topodata,
                                                logger,
                                                parsed_serviceendpoint_contacts,
                                                group_endpoints))
    ]

    executor = ProcessPoolExecutor(max_workers=2)
    group_groups, group_endpoints = await asyncio.gather(*attach_contacts_workers, loop=loop)

    if parsed_servicegroups_contacts:
        attach_contacts_topodata(logger, parsed_servicegroups_contacts, group_groups)
    elif fetched_servicegroups:
        # GOCDB has not SERVICEGROUP_CONTACTS, try to grab contacts from fetched
        # servicegroups topology entities
        parsed_servicegroups_contacts = parse_source_servicegroupscontacts(logger, fetched_servicegroups, custname)
        attach_contacts_topodata(logger, parsed_servicegroups_contacts, group_groups)

    await write_state(connector_name, globopts, confcust, fixed_date, True)

    numge = len(group_endpoints)
    numgg = len(group_groups)

    # send concurrently to WEB-API in coroutines
    if eval(globopts['GeneralPublishWebAPI'.lower()]):
        await asyncio.gather(
            send_webapi(logger, connector_name, globopts, webapi_opts, group_groups, 'groups', fixed_date),
            send_webapi(logger, connector_name, globopts, webapi_opts, group_endpoints,'endpoints', fixed_date)
        )

    if eval(globopts['GeneralWriteAvro'.lower()]):
        write_avro(logger, globopts, confcust, group_groups, group_endpoints, fixed_date)

    logger.info('Customer:' + custname + ' Type:%s ' % (','.join(topofetchtype)) + 'Fetched Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))
