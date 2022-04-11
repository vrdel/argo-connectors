import xml.dom.minidom

from argo_egi_connectors.parse.gocdb_topology import ParseServiceGroups, ParseServiceEndpoints, ParseSites
from argo_egi_connectors.parse.gocdb_contacts import ParseSiteContacts, ParseServiceEndpointContacts, ParseServiceGroupRoles, ParseSitesWithContacts, ParseServiceGroupWithContacts

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.io.ldap import LDAPSessionWithRetry
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write


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


def parse_source_servicegroupsroles(res, custname):
    contacts = ParseServiceGroupRoles(logger, res)
    return contacts.get_contacts()


def parse_source_serviceendpoints_contacts(logger, res, custname):
    contacts = ParseServiceEndpointContacts(logger, res)
    return contacts.get_contacts()


async def fetch_data(logger, connector_name, api, auth_opts, paginated):
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
        session = SessionWithRetry(logger, os.path.basename(sys.argv[0]),
                                   globopts, custauth=auth_opts)
        res = await session.http_get(api)
        return res


async def send_webapi(webapi_opts, data, topotype, fixed_date=None):
    webapi = WebAPI(sys.argv[0], webapi_opts['webapihost'],
                    webapi_opts['webapitoken'], logger,
                    int(globopts['ConnectionRetry'.lower()]),
                    int(globopts['ConnectionTimeout'.lower()]),
                    int(globopts['ConnectionSleepRetry'.lower()]),
                    date=fixed_date)
    await webapi.send(data, topotype)


async def run(loop, logger, connector_name, globopts, auth_opts, webapi_opts,
              confcust, custname, topofetchtype, fixed_date, uidservendp):
    fetched_sites, fetched_servicegroups, fetched_endpoints = None, None, None
    fetched_bdii = None

    group_endpoints, group_groups = list(), list()
    parsed_site_contacts, parsed_servicegroups_contacts, parsed_serviceendpoint_contacts = None, None, None

    try:
        contact_coros = [
            fetch_data(logger, connector_name, topofeed + SITE_CONTACTS, auth_opts, False),
            fetch_data(logger, connector_name, topofeed + SERVICEGROUP_CONTACTS, auth_opts, False)
        ]
        contacts = loop.run_until_complete(asyncio.gather(*contact_coros, return_exceptions=True))

        exc_raised, exc = contains_exception(contacts)
        if exc_raised:
            raise ConnectorHttpError(repr(exc))

        parsed_site_contacts = parse_source_sitescontacts(contacts[0], custname)
        parsed_servicegroups_contacts = parse_source_servicegroupsroles(contacts[1], custname)


    except (ConnectorHttpError, ConnectorParseError) as exc:
        logger.warn('SITE_CONTACTS and SERVICERGOUP_CONTACT methods not implemented')


    coros = [fetch_data(SERVICE_ENDPOINTS_PI, auth_opts, topofeedpaging)]
    if 'servicegroups' in topofetchtype:
        coros.append(fetch_data(SERVICE_GROUPS_PI, auth_opts, topofeedpaging))
    if 'sites' in topofetchtype:
        coros.append(fetch_data(SITES_PI, auth_opts, topofeedpaging))

    if bdii_opts and eval(bdii_opts['bdii']):
        host = bdii_opts['bdiihost']
        port = bdii_opts['bdiiport']
        base = bdii_opts['bdiiquerybase']

        coros.append(fetch_ldap_data(host, port, base,
                                        bdii_opts['bdiiqueryfiltersrm'],
                                        bdii_opts['bdiiqueryattributessrm'].split(' ')))

        coros.append(fetch_ldap_data(host, port, base,
                                        bdii_opts['bdiiqueryfiltersepath'],
                                        bdii_opts['bdiiqueryattributessepath'].split(' ')))

    # fetch topology data concurrently in coroutines
    fetched_topology = loop.run_until_complete(asyncio.gather(*coros, return_exceptions=True))

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
    exe_parse_source_endpoints = partial(parse_source_endpoints,
                                            fetched_endpoints, custname,
                                            uidservendp, pass_extensions)
    exe_parse_source_servicegroups = partial(parse_source_servicegroups,
                                                fetched_servicegroups,
                                                custname, uidservendp,
                                                pass_extensions)
    exe_parse_source_sites = partial(parse_source_sites, fetched_sites,
                                        custname, uidservendp,
                                        pass_extensions)

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

    parsed_topology = loop.run_until_complete(asyncio.gather(*parse_workers))

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
    parsed_serviceendpoint_contacts = parse_source_serviceendpoints_contacts(fetched_endpoints, custname)

    if not parsed_site_contacts and fetched_sites:
        # GOCDB has not SITE_CONTACTS, try to grab contacts from fetched
        # sites topology entities
        parsed_site_contacts = parse_source_siteswithcontacts(fetched_sites, custname)

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
    group_groups, group_endpoints = loop.run_until_complete(asyncio.gather(*attach_contacts_workers))

    if parsed_servicegroups_contacts:
        attach_contacts_topodata(logger, parsed_servicegroups_contacts, group_groups)
    elif fetched_servicegroups:
        # GOCDB has not SERVICEGROUP_CONTACTS, try to grab contacts from fetched
        # servicegroups topology entities
        parsed_servicegroups_contacts = parse_source_servicegroupscontacts(fetched_servicegroups, custname)
        attach_contacts_topodata(logger, parsed_servicegroups_contacts, group_groups)

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

    logger.info('Customer:' + custname + ' Type:%s ' % (','.join(topofetchtype)) + 'Fetched Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))

