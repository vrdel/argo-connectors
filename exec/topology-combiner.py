#!/usr/bin/env python

import argparse
import asyncio
import os
import sys

from argo_connectors.log import Logger
from argo_connectors.config.combine import CombineConf

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import Customer, BDIIOpts, WebAPIOpts, AuthOpts

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError, ConnectorConfError
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
    parser = argparse.ArgumentParser(description="""Combiner that calls topology tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""")
    parser.add_argument('-c', dest='yamlconf', metavar='combine.yml',
                        help='path to YAML file', type=str, required=True)
    args = parser.parse_args()

    logger = Logger(os.path.basename(sys.argv[0]))

    combopts = CombineConf(sys.argv[0], args.yamlconf).parse()

    try:
        globopts = Global(sys.argv[0]).options()
        confcust = Customer(sys.argv[0])
        confcust.valid()

    except ConnectorConfError as exc:
        logger.error(exc)
        raise SystemExit(1)


if __name__ == '__main__':
    main()
