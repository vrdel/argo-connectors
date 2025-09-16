#!/usr/bin/env python

import argparse
import asyncio
import os
import sys

from argo_connectors.log import Logger
from argo_connectors.config.combine import CombineConf

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import Customer, BDIIOpts, WebAPIOpts, AuthOpts

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.tasks.common import write_state
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology
from argo_connectors.utils import date_check


async def runme(task):
    coros = list()
    coros.append(task.run())
    coros.append(task.run())
    coros.append(task.run())
    foo = await asyncio.gather(*coros)
    print(foo)


def main():
    # parser = argparse.ArgumentParser(description="""Combiner that calls topology tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""")
    # parser.add_argument('-c', dest='yamlconf', metavar='combine.yml',
                        # help='path to YAML file', type=str, required=True)
    # args = parser.parse_args()
    # logger = Logger(os.path.basename(sys.argv[0]))

    # combopts = CombineConf(sys.argv[0], args.yamlconf).parse()

    parser = argparse.ArgumentParser(description="""Fetch entities (ServiceGroups, Sites, Endpoints)
                                                    from GOCDB for every customer and job listed in customer.conf and write them
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
    globopts = Global("topology-gocdb-connector.py", confpath).options()

    confpath = args.custconf if args.custconf else None
    confcust = Customer("topology-gocdb-connector.py", confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    logger.customer = confcust.get_custname()

    auth_conf = AuthOpts()
    if not auth_conf.opts:
        logger.error('%s options incomplete, missing %s' %
                     ('authentication', ' '.join(auth_conf.missing)))
        raise SystemExit(1)

    bdii_conf = BDIIOpts()
    if bdii_conf.missing:
        logger.error('%s options incomplete, missing %s' %
                     ('bdii', ' '.join(bdii_conf.missing)))
        raise SystemExit(1)

    webapi_conf = WebAPIOpts()
    if not webapi_conf.opts:
        logger.error('%s options incomplete, missing %s' %
                     ('webapi', ' '.join(webapi_conf.missing)))
        raise SystemExit(1)

    loop = asyncio.get_event_loop()
    try:
        task = TaskGocdbTopology(loop, logger, fixed_date)
        # loop.run_until_complete(task.run())
        loop.run_until_complete(runme(task))

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        loop.run_until_complete(
            write_state(confcust, fixed_date, False)
        )

    finally:
        loop.close()

if __name__ == '__main__':
    main()
