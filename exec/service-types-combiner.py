#!/usr/bin/env python

import asyncio
import sys

from argo_connectors.exe.combiner import ExecCombiner

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.tasks.gocdb_servicetypes import TaskGocdbServiceTypes
from argo_connectors.tasks.flat_servicetypes import TaskFlatServiceTypes
from argo_connectors.tasks.common import write_state, write_servicetypes_json as write_json
from argo_connectors.io.webapi import WebAPI


async def fetch(tasks):
    fetched_data = await asyncio.gather(*tasks)
    return fetched_data


async def webapi_send(logger, servicetypes, combuid):
    webapi = WebAPI(logger, combuid=combuid)
    await webapi.send(servicetypes, 'service-types')
    await webapi.session.close()


def combine(servicetypes):
    joint_servicetypes = list()

    for st in servicetypes:
        joint_servicetypes += st

    return joint_servicetypes


def main():
    combine_exec = ExecCombiner(
        description="""Combiner that calls service-types tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""",
        exe_script=sys.argv[0],
        combiner='service-types'
    )
    coros = list()

    for task in combine_exec.tasks:
        if task['type'] == 'gocdb':
            coros.append(TaskGocdbServiceTypes(combine_exec.logger, None,
                                               initsync=False,
                                               combuid=task['id']).run())
        elif task['type'] == 'csv':
            coros.append(TaskFlatServiceTypes(combine_exec.logger, None, True,
                                              initsync=False,
                                              combuid=task['id']).run())
        elif task['type'] == 'json':
            coros.append(TaskFlatServiceTypes(combine_exec.logger, None, False,
                                              initsync=False,
                                              combuid=task['id']).run())

    try:
        data_fetched = asyncio.run(fetch(coros))
        servicetypes = combine(data_fetched)
        asyncio.run(write_state(None, True, task['id']))

        numst = len(servicetypes)

        combine_exec.logger.info('Customer:' + combine_exec.tenant_name + ' Joined ServiceTypes:%d' % (numst))

        if combine_exec.globopts.options()['GeneralWriteJson'.lower()]:
            write_json(combine_exec.logger, servicetypes, None, task['id'])

        if combine_exec.globopts.options()['GeneralPublishWebAPI'.lower()]:
            asyncio.run(webapi_send(combine_exec.logger, servicetypes, task['id']))

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        combine_exec.logger.error(repr(exc))
        asyncio.run(write_state(None, False, task['id']))


if __name__ == '__main__':
    main()
