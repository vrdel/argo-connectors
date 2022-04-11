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
import os
import sys

from concurrent.futures import ProcessPoolExecutor
from functools import partial
from urllib.parse import urlparse

import asyncio
import uvloop

from argo_egi_connectors.exceptions import ConnectorParseError, ConnectorHttpError
from argo_egi_connectors.log import Logger
from argo_egi_connectors.mesh.srm_port import attach_srmport_topodata
from argo_egi_connectors.mesh.storage_element_path import attach_sepath_topodata
from argo_egi_connectors.mesh.contacts import attach_contacts_topodata

from argo_egi_connectors.config import Global, CustomerConf

from argo_egi_connectors.utils import filename_date, date_check

logger = None

# GOCDB explicitly says &scope='' for all scopes
# TODO: same methods can be served on different paths
SERVICE_ENDPOINTS_PI = '/gocdbpi/private/?method=get_service_endpoint&scope='
SITES_PI = '/gocdbpi/private/?method=get_site&scope='
SERVICE_GROUPS_PI = '/gocdbpi/private/?method=get_service_group&scope='

# SITES_PI = '/vapor/downloadLavoisier/option/xml/view/vapor_sites/param/vo=biomed'
# SERVICE_ENDPOINTS_PI = '/vapor/downloadLavoisier/option/xml/view/vapor_endpoints'

ROC_CONTACTS = '/gocdbpi/private/?method=get_roc_contacts'
SITE_CONTACTS = '/gocdbpi/private/?method=get_site_contacts'
PROJECT_CONTACTS = '/gocdbpi/private/?method=get_project_contacts'
SERVICEGROUP_CONTACTS = '/gocdbpi/private/?method=get_service_group_role'

globopts = {}
custname = ''

isok = True


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


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


def get_bdii_opts(confcust):
    bdii_custopts = confcust._get_cust_options('BDIIOpts')
    if bdii_custopts:
        bdii_complete, missing = confcust.is_complete_bdii(bdii_custopts)
        if not bdii_complete:
            logger.error('%s options incomplete, missing %s' % ('bdii', ' '.join(missing)))
            raise SystemExit(1)
        return bdii_custopts
    else:
        return None


# Fetches data from LDAP, connection parameters are set in customer.conf
async def fetch_ldap_data(host, port, base, filter, attributes):
    ldap_session = LDAPSessionWithRetry(logger, int(globopts['ConnectionRetry'.lower()]),
        int(globopts['ConnectionSleepRetry'.lower()]), int(globopts['ConnectionTimeout'.lower()]))

    res = await ldap_session.search(host, port, base, filter, attributes)
    return res


def contains_exception(list):
    for a in list:
        if isinstance(a, Exception):
            return (True, a)

    return (False, None)


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
    pass_extensions = eval(globopts['GeneralPassExtensions'.lower()])

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    topofeed = confcust.get_topofeed()
    topofeedpaging = confcust.get_topofeedpaging()
    uidservendp = confcust.get_uidserviceendpoints()
    topofetchtype = confcust.get_topofetchtype()
    custname = confcust.get_custname()
    logger.customer = custname

    auth_custopts = confcust.get_authopts()
    auth_opts = cglob.merge_opts(auth_custopts, 'authentication')
    auth_complete, missing = cglob.is_complete(auth_opts, 'authentication')
    if not auth_complete:
        logger.error('%s options incomplete, missing %s' % ('authentication', ' '.join(missing)))
        raise SystemExit(1)

    bdii_opts = get_bdii_opts(confcust)

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        toposcope = confcust.get_toposcope()
        topofeedendpoints = confcust.get_topofeedendpoints()
        topofeedservicegroups = confcust.get_topofeedservicegroups()
        topofeedsites = confcust.get_topofeedsites()
        global SERVICE_ENDPOINTS_PI, SERVICE_GROUPS_PI, SITES_PI
        if toposcope:
            SERVICE_ENDPOINTS_PI = SERVICE_ENDPOINTS_PI + toposcope
            SERVICE_GROUPS_PI = SERVICE_GROUPS_PI + toposcope
            SITES_PI = SITES_PI + toposcope
        if topofeedendpoints:
            SERVICE_ENDPOINTS_PI = topofeedendpoints
        else:
            SERVICE_ENDPOINTS_PI = topofeed + SERVICE_ENDPOINTS_PI
        if topofeedservicegroups:
            SERVICE_GROUPS_PI = topofeedservicegroups
        else:
            SERVICE_GROUPS_PI = topofeed + SERVICE_GROUPS_PI
        if topofeedsites:
            SITES_PI = topofeedsites
        else:
            SITES_PI = topofeed + SITES_PI

    except (ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(confcust, fixed_date, False)
        )

    finally:
        loop.close()


if __name__ == '__main__':
    main()
