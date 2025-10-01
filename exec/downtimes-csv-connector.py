#!/usr/bin/env python

import argparse
import datetime
import os
import sys

import asyncio

from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError, ConnectorConfError
from argo_connectors.log import Logger
from argo_connectors.tasks.flat_downtimes import TaskCsvDowntimes
from argo_connectors.tasks.common import write_state

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import Customer


def main():
    parser = argparse.ArgumentParser(description='Fetch downtimes from CSV for given date')
    parser.add_argument('-c', dest='custconf', metavar='customer.conf',
                        default=None, help='path to customer configuration file',
                        type=str, required=False)
    parser.add_argument('-g', dest='gloconf', metavar='global.conf',
                        default=None, help='path to global configuration file',
                        type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY',
                        help='write data for this date', type=str, required=True)
    args = parser.parse_args()
    logger = Logger(os.path.basename(sys.argv[0]))

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

    # calculate start and end times
    try:
        current_date = datetime.datetime.strptime(args.date, '%Y-%m-%d')
        timestamp = current_date.strftime('%Y_%m_%d')
        current_date = current_date.replace(hour=0, minute=0, second=0)

    except ValueError as exc:
        logger.error(exc)
        raise SystemExit(1)

    try:
        task = TaskCsvDowntimes(logger, current_date, args.date, timestamp)
        asyncio.run(task.run())

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        asyncio.run(write_state(timestamp, False))


if __name__ == '__main__':
    main()
