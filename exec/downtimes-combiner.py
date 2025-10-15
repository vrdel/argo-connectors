#!/usr/bin/env python

import argparse
import asyncio
import os
import sys
import datetime

from argo_connectors.log import Logger
from argo_connectors.config.combine import CombineConf

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import CombinerCustomer

from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError, ConnectorConfError
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
    parser = argparse.ArgumentParser(description="""Combiner that calls service-types tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""")
    parser.add_argument('-c', dest='yamlconf', metavar='combine.yml',
                        help='path to YAML file', type=str, required=True)
    args = parser.parse_args()

    logger = Logger(os.path.basename(sys.argv[0]))

    combopts = CombineConf(sys.argv[0], args.yamlconf).parse()

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
        logger.error(exc)
        raise SystemExit(1)

    for comb in combopts:
        try:
            comb_globopts = comb.get('config', None)
            globopts = Global(sys.argv[0])
            if comb_globopts:
                globopts.configure(comb_globopts)
            downtimes_confs = comb.get('combine')
            n = 1
            if downtimes_confs:
                for dt_conf in downtimes_confs:
                    which = dt_conf.get('type', None)
                    if not which:
                        raise ConnectorConfError('type is mandatory in downtimes combine')
                    combuid = f'{n}-{which}'
                    confcust = CombinerCustomer(sys.argv[0], combuid, comb['tenant'])
                    confcust.configure(dt_conf)
                    confcust.valid()
                    confcust.make_dirstruct(jobdir=False)
                    confcust.make_dirstruct(globopts.options()['InputStateSaveDir'.lower()], jobdir=False)
                    logger.customer = comb['tenant']
                    if which.lower() == 'gocdb':
                        coros.append(TaskGocdbDowntimes(logger, start, end,
                                                        current_date,
                                                        timestamp,
                                                        combuid=combuid).run())
                    elif which.lower() == 'csv':
                        coros.append(TaskCsvDowntimes(logger, start, end,
                                                      current_date, timestamp,
                                                      combuid=combuid).run())
                    n += 1
            else:
                raise ConnectorConfError('combine key mandatory')

        except ConnectorConfError as exc:
            logger.error(exc)
            raise SystemExit(1)

    try:
        data_fetched = asyncio.run(fetch(coros))
        downtimes = combine(data_fetched)
        asyncio.run(write_state(None, True, combuid))

        numst = len(downtimes)

        logger.info('Customer:' + comb['tenant'] + ' Joined Downtimes:%d' % (numst))

        if globopts.options()['GeneralWriteJson'.lower()]:
            write_json(logger, downtimes, None, combuid)

        if globopts.options()['GeneralPublishWebAPI'.lower()]:
            asyncio.run(webapi_send(logger, downtimes, combuid))

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        asyncio.run(write_state(None, False, combuid))


if __name__ == '__main__':
    main()
