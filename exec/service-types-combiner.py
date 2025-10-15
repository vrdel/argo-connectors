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
    parser = argparse.ArgumentParser(description="""Combiner that calls service-types tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""")
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
            servtype_confs = comb.get('combine')
            n = 1
            if servtype_confs:
                for st_conf in servtype_confs:
                    which = st_conf.get('type', None)
                    if not which:
                        raise ConnectorConfError('type is mandatory in service-types combine')
                    combuid = f'{n}-{which}'
                    confcust = CombinerCustomer(sys.argv[0], combuid, comb['tenant'])
                    confcust.configure(st_conf)
                    confcust.valid()
                    confcust.make_dirstruct(jobdir=False)
                    confcust.make_dirstruct(globopts.options()['InputStateSaveDir'.lower()], jobdir=False)
                    logger.customer = comb['tenant']
                    if which.lower() == 'gocdb':
                        coros.append(TaskGocdbServiceTypes(logger, None,
                                                           initsync=False,
                                                           combuid=combuid).run())
                    elif which.lower() == 'csv':
                        coros.append(TaskFlatServiceTypes(logger, None, True,
                                                          initsync=False,
                                                          combuid=combuid).run())
                    elif which.lower() == 'json':
                        coros.append(TaskFlatServiceTypes(logger, None, False,
                                                          initsync=False,
                                                          combuid=combuid).run())
                    n += 1
            else:
                raise ConnectorConfError('combine key mandatory')

        except ConnectorConfError as exc:
            logger.error(exc)
            raise SystemExit(1)

    try:
        data_fetched = asyncio.run(fetch(coros))
        servicetypes = combine(data_fetched)
        asyncio.run(write_state(None, True, combuid))

        numst = len(servicetypes)

        logger.info('Customer:' + comb['tenant'] + ' Joined ServiceTypes:%d' % (numst))

        if globopts.options()['GeneralWriteJson'.lower()]:
            write_json(logger, servicetypes, None, combuid)

        if globopts.options()['GeneralPublishWebAPI'.lower()]:
            asyncio.run(webapi_send(logger, servicetypes, combuid))

    except (ConnectorError, ConnectorParseError, ConnectorHttpError, KeyboardInterrupt) as exc:
        logger.error(repr(exc))
        asyncio.run(write_state(None, False, combuid))


if __name__ == '__main__':
    main()
