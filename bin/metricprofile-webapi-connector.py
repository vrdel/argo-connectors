#!/usr/bin/python3

import argparse
import os
import re
import sys

import uvloop
import asyncio

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_egi_connectors.io.avrowrite import AvroWriter
from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.tasks.webapi_metricprofile import TaskWebApiMetricProfile
from argo_egi_connectors.log import Logger

from argo_egi_connectors.config import CustomerConf, Global
from argo_egi_connectors.utils import filename_date, module_class_name, datestamp, date_check
from argo_egi_connectors.parse.webapi_metricprofile import ParseMetricProfiles

logger = None

globopts = dict()
custname = ''
API_PATH = '/api/v2/metric_profiles'


async def fetch_data(host, token):
    session = SessionWithRetry(logger, os.path.basename(sys.argv[0]), globopts,
                               token=token)
    res = await session.http_get('{}://{}{}'.format('https', host, API_PATH))
    return res


def parse_source(res, profiles, namespace):
    metric_profiles = ParseMetricProfiles(logger, res, profiles, namespace).get_data()
    return metric_profiles


def main():
    global logger, globopts
    parser = argparse.ArgumentParser(description='Fetch metric profile for every job of the customer')
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    args = parser.parse_args()

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

    loop = uvloop.new_event_loop()
    asyncio.set_event_loop(loop)

    for cust in confcust.get_customers():
        custname = confcust.get_custname(cust)
        task = TaskWebApiMetricProfile(
        )
        loop.run_until_complete(task.run())


if __name__ == '__main__':
    main()
