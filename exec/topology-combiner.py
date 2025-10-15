#!/usr/bin/env python

import argparse
import asyncio
import os
import sys

from argo_connectors.log import Logger
from argo_connectors.config.combine import CombineConf

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import CombinerCustomer

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError, ConnectorConfError
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology
from argo_connectors.tasks.lot1sc_topology import TaskLot1ScTopology
from argo_connectors.tasks.provider_topology import TaskProviderTopology
from argo_connectors.tasks.flat_topology import TaskFlatTopology
from argo_connectors.tasks.common import write_state, write_topo_json as write_json
from argo_connectors.io.webapi import WebAPI


async def fetch(tasks):
    fetched_data = await asyncio.gather(*tasks)
    return fetched_data


async def webapi_send(logger, group_groups, group_endpoints, combuid):
    webapi = WebAPI(logger, combuid=combuid)
    await asyncio.gather(
        webapi.send(group_groups, 'groups'),
        webapi.send(group_endpoints, 'endpoints')
    )
    await webapi.session.close()


def combine(topologies):
    joint_gg, joint_ge = list(), list()

    for topo in topologies:
        group_groups, group_endpoints = topo
        joint_gg += group_groups
        joint_ge += group_endpoints

    return joint_gg, joint_ge


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
            n = 1
            if topos_confs:
                for topoconf in topos_confs:
                    which = topoconf.get('type', None)
                    if not which:
                        raise ConnectorConfError('type is mandatory in topology combine')
                    combuid = f'{n}-{which}'
                    confcust = CombinerCustomer(sys.argv[0], combuid, comb['tenant'])
                    confcust.configure(topoconf)
                    confcust.valid()
                    confcust.make_dirstruct(jobdir=False)
                    confcust.make_dirstruct(globopts.options()['InputStateSaveDir'.lower()], jobdir=False)
                    logger.customer = comb['tenant']
                    if which.lower() == 'gocdb':
                        coros.append(TaskGocdbTopology(logger, None, combuid).run())
                    elif which.lower() == 'lot1sc':
                        coros.append(TaskLot1ScTopology(logger, None, combuid).run())
                    elif which.lower() == 'provider':
                        coros.append(TaskProviderTopology(logger, None, combuid).run())
                    elif which.lower() == 'csv':
                        coros.append(TaskFlatTopology(logger, None, True, combuid=combuid).run())
                    elif which.lower() == 'json':
                        coros.append(TaskFlatTopology(logger, None, False, combuid=combuid).run())
                    n += 1
            else:
                raise ConnectorConfError('combine key mandatory')

        except ConnectorConfError as exc:
            logger.error(exc)
            raise SystemExit(1)

    try:
        data_fetched = asyncio.run(fetch(coros))
        group_groups, group_endpoints = combine(data_fetched)

        asyncio.run(write_state(None, True, combuid))

        numge = len(group_endpoints)
        numgg = len(group_groups)

        logger.info('Customer:' + comb['tenant'] + ' Joined Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))

        if globopts.options()['GeneralWriteJson'.lower()]:
            write_json(logger, group_groups, group_endpoints, None, combuid)

        if globopts.options()['GeneralPublishWebAPI'.lower()]:
            asyncio.run(webapi_send(logger, group_groups, group_endpoints, combuid))

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        asyncio.run(write_state(None, False, combuid))


if __name__ == '__main__':
    main()
