#!/usr/bin/env python

import asyncio
import sys

from argo_connectors.exe.combiner import ExecCombiner

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.io.webapi import WebAPI
from argo_connectors.log import Logger
from argo_connectors.tasks.common import write_state, write_topo_json as write_json
from argo_connectors.tasks.flat_topology import TaskFlatTopology
from argo_connectors.tasks.gocdb_topology import TaskGocdbTopology
from argo_connectors.tasks.lot1sc_topology import TaskLot1ScTopology
from argo_connectors.tasks.provider_topology import TaskProviderTopology


async def fetch(tasks):
    fetched_data = await asyncio.gather(*tasks)
    return fetched_data


async def webapi_send(group_groups, group_endpoints, combuid):
    webapi = WebAPI(combuid=combuid)
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
    combine_exec = ExecCombiner(
        description="""Combiner that calls topology tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""",
        exe_script=sys.argv[0],
        combiner='topology'
    )
    coros = list()

    for task in combine_exec.tasks:
        if task['type'] == 'gocdb':
            coros.append(TaskGocdbTopology(None, task['id']).run())
        elif task['type'] == 'lot1sc':
            coros.append(TaskLot1ScTopology(None, task['id']).run())
        elif task['type'] == 'provider':
            coros.append(TaskProviderTopology(None, task['id']).run())
        elif task['type'] == 'csv':
            coros.append(TaskFlatTopology(None, True,
                                          combuid=task['id']).run())
        elif task['type'] == 'json':
            coros.append(TaskFlatTopology(None, False,
                                          combuid=task['id']).run())

    try:
        data_fetched = asyncio.run(fetch(coros))
        group_groups, group_endpoints = combine(data_fetched)

        asyncio.run(write_state(None, True, task['id']))

        numge = len(group_endpoints)
        numgg = len(group_groups)

        Logger.info('Customer:' + combine_exec.tenant_name + ' Joined Endpoints:%d' % (numge) + ' Groups:%d' % (numgg))

        if combine_exec.globopts.options()['GeneralWriteJson'.lower()]:
            write_json(group_groups, group_endpoints, None, task['id'])

        if combine_exec.globopts.options()['GeneralPublishWebAPI'.lower()]:
            asyncio.run(webapi_send(group_groups, group_endpoints, task['id']))

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        Logger.error(repr(exc))
        asyncio.run(write_state(None, False, task['id']))


if __name__ == '__main__':
    main()
