#!/usr/bin/env python

import argparse
import asyncio
import os
import sys
import contextvars
import uuid

from argo_connectors.log import Logger
from argo_connectors.config.combine import CombineConf

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import CombinerCustomer

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError, ConnectorConfError
from argo_connectors.tasks.common import write_state
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology
from argo_connectors.tasks.lot1sc_topology import TaskLot1ScTopology
from argo_connectors.utils import date_check


async def runme(tasks):
    foo = await asyncio.gather(*tasks)
    print(foo)


def main():
    parser = argparse.ArgumentParser(description="""Combiner that calls topology tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""")
    parser.add_argument('-c', dest='yamlconf', metavar='combine.yml',
                        help='path to YAML file', type=str, required=True)
    args = parser.parse_args()

    logger = Logger(os.path.basename(sys.argv[0]))

    combopts = CombineConf(sys.argv[0], args.yamlconf).parse()

    coros = list()

    for comb in combopts:
        try:
            comb_globopts = comb.get('config', None)
            globopts = Global(sys.argv[0])
            if comb_globopts:
                globopts.configure(comb_globopts)
            topos_confs = comb.get('combine')
            if topos_confs:
                for topoconf in topos_confs:
                    which = topoconf.get('type', None)
                    if not which:
                        raise ConnectorConfError('type is mandatory in topology combine')
                    combuid = f'{which}-{topos_confs.index(topoconf) + 1}'
                    confcust = CombinerCustomer(sys.argv[0], combuid, comb['tenant'])
                    confcust.configure(topoconf)
                    confcust.valid()
                    logger.customer = comb['tenant']
                    if which == 'gocdb':
                        coros.append(TaskGocdbTopology(logger, None, combuid).run())
                    elif which == 'lot1sc':
                        coros.append(TaskLot1ScTopology(logger, None, combuid).run())

        except ConnectorConfError as exc:
            logger.error(exc)
            raise SystemExit(1)

    try:
        asyncio.run(runme(coros))

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        asyncio.run(write_state(None, False))


if __name__ == '__main__':
    main()
