#!/usr/bin/python3

import argparse
import datetime
import os
import sys

import uvloop
import asyncio

from argo_egi_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.log import Logger
from argo_egi_connectors.tasks.gocdb_downtimes import TaskGocdbDowntimes

from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.utils import filename_date, module_class_name

logger = None
globopts = {}

DOWNTIMEPI = '/gocdbpi/private/?method=get_downtime'


def get_webapi_opts(cglob, confcust):
    webapi_custopts = confcust.get_webapiopts()
    webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
    webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')
    if not webapi_complete:
        logger.error('Customer:%s %s options incomplete, missing %s' % (logger.customer, 'webapi', ' '.join(missopt)))
        raise SystemExit(1)
    return webapi_opts


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description='Fetch downtimes from GOCDB for given date')
    parser.add_argument('-d', dest='date', nargs=1, metavar='YEAR-MONTH-DAY', required=True)
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    args = parser.parse_args()

    logger = Logger(os.path.basename(sys.argv[0]))
    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    feed = confcust.get_topofeed()
    logger.customer = confcust.get_custname()

    if len(args.date) == 0:
        print(parser.print_help())
        raise SystemExit(1)

    # calculate start and end times
    try:
        start = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        end = datetime.datetime.strptime(args.date[0], '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=23, minute=59, second=59)

    except ValueError as exc:
        logger.error(exc)
        raise SystemExit(1)

    uidservtype = confcust.get_uidserviceendpoints()

    webapi_opts = get_webapi_opts(cglob, confcust)

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        task = TaskGocdbDowntimes(loop, logger, connector_name, globopts,
                                  webapi_opts, confcust, custname, feed,
                                  DOWNTIMEPI, start, end, uidservtype, targetdate, timestamp)

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
            write_state(confcust, timestamp, False)
        )

    loop.close()

if __name__ == '__main__':
    main()
