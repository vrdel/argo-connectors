#!/usr/bin/env python

import argparse
import os
import sys

import asyncio

from argo_connectors.config.customer import Customer
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError, ConnectorConfError
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state
from argo_connectors.tasks.flat_topology import TaskFlatTopology
from argo_connectors.utils import date_check


def main():
    global logger, globopts, confcust
    parser = argparse.ArgumentParser(description="""Fetch entities (ServiceGroups, Sites, Endpoints)
                                                    from CSV topology feed for every customer and job listed in customer.conf and write them
                                                    in an appropriate place""")
    parser.add_argument('-c', dest='custconf', metavar='customer.conf',
                        default=None, help='path to customer configuration file',
                        type=str, required=False)
    parser.add_argument('-g', dest='gloconf', metavar='global.conf',
                        default=None, help='path to global configuration file',
                        type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY',
                        help='write data for this date', type=str, required=False)
    args = parser.parse_args()
    logger = Logger(os.path.basename(sys.argv[0]))

    fixed_date = None
    if args.date and date_check(args.date):
        fixed_date = args.date

    confpath = args.gloconf if args.gloconf else None

    try:
        globopts = Global(sys.argv[0], confpath).options()
        confpath = args.custconf if args.custconf else None
        confcust = Customer(sys.argv[0], confpath)
        confcust.valid()

    except ConnectorConfError as exc:
        logger.error(exc)
        raise SystemExit(1)

    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    logger.customer = confcust.get_custname()

    try:
        task = TaskFlatTopology(logger, fixed_date, is_csv=False)
        asyncio.run(task.run())

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        asyncio.run(write_state(fixed_date, False))


if __name__ == '__main__':
    main()
