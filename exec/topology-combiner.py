#!/usr/bin/env python

import argparse
import os
import sys

import asyncio

from argo_connectors.config import Global, CustomerConf
from argo_connectors.exceptions import ConnectorError, ConnectorParseError, ConnectorHttpError
from argo_connectors.log import Logger


def main():
    parser = argparse.ArgumentParser(description="""Combiner that calls topology tasks specified in YAML file, joins their data, record it in JSON file and push it to WEB-API""")
    parser.add_argument('-c', dest='yamlconf', nargs=1, metavar='combine.yml',
                        help='path to YAML file', type=str, required=True)
    args = parser.parse_args()
    logger = Logger(os.path.basename(sys.argv[0]))


if __name__ == '__main__':
    main()
