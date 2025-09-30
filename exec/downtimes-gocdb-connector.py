#!/usr/bin/env python

import argparse
import datetime
import os
import sys

import asyncio

from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError, ConnectorConfError
from argo_connectors.log import Logger
from argo_connectors.tasks.gocdb_downtimes import TaskGocdbDowntimes
from argo_connectors.tasks.common import write_state

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import Customer


def main():
    parser = argparse.ArgumentParser(
        description='Fetch downtimes from GOCDB for given date')
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
        start = datetime.datetime.strptime(args.date, '%Y-%m-%d')
        end = datetime.datetime.strptime(args.date, '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=23, minute=59, second=59)

    except ValueError as exc:
        logger.error(exc)
        raise SystemExit(1)

    downtime_feed = confcust.opt('DowntimesFeed')
    toposcope = confcust.opt('TopoScope')
    if toposcope and '&scope=' not in toposcope:
        downtime_feed += '&scope={}'.format(toposcope)
    elif toposcope and '&scope=' in toposcope:
        downtime_feed += toposcope

    try:
        task = TaskGocdbDowntimes(logger, start, end, args.date, timestamp)
        asyncio.run(task.run())

    except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        asyncio.run(write_state(timestamp, False))


if __name__ == '__main__':
    main()
