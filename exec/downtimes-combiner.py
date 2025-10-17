#!/usr/bin/env python

import asyncio
import sys
import datetime

from argo_connectors.exe.combiner import ExecCombiner

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.tasks.gocdb_downtimes import TaskGocdbDowntimes
from argo_connectors.tasks.flat_downtimes import TaskCsvDowntimes
from argo_connectors.tasks.common import write_state, write_downtimes_json as write_json
from argo_connectors.io.webapi import WebAPI


async def fetch(tasks):
    fetched_data = await asyncio.gather(*tasks)
    return fetched_data


async def webapi_send(logger, downtimes, combuid):
    webapi = WebAPI(logger, combuid=combuid)
    await webapi.send(downtimes, 'downtimes')
    await webapi.session.close()


def combine(downtimes):
    joint_downtimes = list()

    for dt in downtimes:
        joint_downtimes += dt

    return joint_downtimes


def main():
    combine_exec = ExecCombiner(
        description="""Combiner that calls downtimes tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""",
        exe_script=sys.argv[0],
        combiner='downtimes'
    )
    coros = list()

    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    # calculate start and end times
    try:
        start = datetime.datetime.strptime(current_date, '%Y-%m-%d')
        end = datetime.datetime.strptime(current_date, '%Y-%m-%d')
        timestamp = start.strftime('%Y_%m_%d')
        start = start.replace(hour=0, minute=0, second=0)
        end = end.replace(hour=23, minute=59, second=59)

    except ValueError as exc:
        combine_exec.logger.error(exc)
        raise SystemExit(1)

    for task in combine_exec.tasks:
        if task['type'] == 'gocdb':
            coros.append(TaskGocdbDowntimes(combine_exec.logger, start, end,
                                            current_date,
                                            timestamp,
                                            combuid=task['id']).run())
        elif task['type'] == 'csv':
            coros.append(TaskCsvDowntimes(combine_exec.logger, start, end,
                                          current_date, timestamp,
                                          combuid=task['id']).run())

    try:
        data_fetched = asyncio.run(fetch(coros))
        downtimes = combine(data_fetched)
        asyncio.run(write_state(None, True, task['id']))

        numst = len(downtimes)

        combine_exec.logger.info('Customer:' + combine_exec.tenant_name + ' Joined Downtimes:%d' % (numst))

        if combine_exec.globopts.options()['GeneralWriteJson'.lower()]:
            write_json(combine_exec.logger, downtimes, None, task['id'])

        if combine_exec.globopts.options()['GeneralPublishWebAPI'.lower()]:
            asyncio.run(webapi_send(combine_exec.logger, downtimes, task['id']))

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        combine_exec.logger.error(repr(exc))
        asyncio.run(write_state(None, False, task['id']))


if __name__ == '__main__':
    main()
