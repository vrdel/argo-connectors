#!/usr/bin/python3

import argparse
import os
import sys
import json

from urllib.parse import urlparse

from argo_egi_connectors import input
from argo_egi_connectors import output
from argo_egi_connectors.log import Logger
from argo_egi_connectors.config import Global, CustomerConf
from argo_egi_connectors.helpers import filename_date, datestamp, date_check


def is_feed(feed):
    data = urlparse(feed)

    if not data.netloc:
        return False
    else:
        return True


class EOSCReader(object):
    def __init__(self, data, uidservtype=False, fetchtype='ServiceGroups'):
        self.data = data
        self.uidservtype = uidservtype
        self.fetchtype = fetchtype

    def _construct_fqdn(self, http_endpoint):
        return urlparse(http_endpoint).netloc

    def get_groupgroups(self):
        groups = list()

        for entity in self.data:
            tmp_dict = dict()

            tmp_dict['type'] = 'PROJECT'
            tmp_dict['group'] = 'EOSC'
            tmp_dict['subgroup'] = entity['SITENAME-SERVICEGROUP']
            tmp_dict['tags'] = {'monitored': '1', 'scope': 'EOSC'}

            groups.append(tmp_dict)

        return groups

    def get_groupendpoints(self):
        groups = list()

        for entity in self.data:
            tmp_dict = dict()

            tmp_dict['type'] = self.fetchtype.upper()

            tmp_dict['service'] = entity['SERVICE_TYPE']
            info_url = entity['URL']
            if self.uidservtype:
                tmp_dict['hostname'] = '{1}_{0}'.format(entity['Service Unique ID'], self._construct_fqdn(info_url))
            else:
                tmp_dict['hostname'] = self._construct_fqdn(entity['URL'])
            tmp_dict['tags'] = {'scope': 'EOSC', 'monitored': '1', 'info.URL': info_url}

            groups.append(tmp_dict)

        return groups


def main():
    parser = argparse.ArgumentParser(description="""Fetch and construct entities from EOSC-PORTAL feed""")
    parser.add_argument('-c', dest='custconf', nargs=1, metavar='customer.conf', help='path to customer configuration file', type=str, required=False)
    parser.add_argument('-g', dest='gloconf', nargs=1, metavar='global.conf', help='path to global configuration file', type=str, required=False)
    parser.add_argument('-d', dest='date', metavar='YEAR-MONTH-DAY', help='write data for this date', type=str, required=False)
    args = parser.parse_args()
    group_endpoints, group_groups = [], []
    logger = Logger(os.path.basename(sys.argv[0]))

    fixed_date = None
    if args.date and date_check(args.date):
        fixed_date = args.date

    confpath = args.gloconf[0] if args.gloconf else None
    cglob = Global(sys.argv[0], confpath)
    globopts = cglob.parse()

    confpath = args.custconf[0] if args.custconf else None
    confcust = CustomerConf(sys.argv[0], confpath)
    confcust.parse()
    confcust.make_dirstruct()
    confcust.make_dirstruct(globopts['InputStateSaveDir'.lower()])
    custname = confcust.get_custname()

    # safely assume here one customer defined in customer file
    cust = list(confcust.get_customers())[0]
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    fetchtype = confcust.get_topofetchtype()

    state = None
    logger.customer = custname

    uidservtype = confcust.get_uidserviceendpoints()

    topofeed = confcust.get_topofeed()
    if is_feed(topofeed):
        remote_topo = urlparse(topofeed)
        res = input.connection(logger, 'EOSC', globopts, remote_topo.scheme, remote_topo.netloc, remote_topo.path)
        if not res:
            state = False
        else:
            doc = input.parse_json(logger, 'EOSC', globopts, res,
                                   remote_topo.scheme + '://' +
                                   remote_topo.netloc + remote_topo.path)
            eosc = EOSCReader(doc, uidservtype, fetchtype)
            group_groups = eosc.get_groupgroups()
            group_endpoints = eosc.get_groupendpoints()
            state = True
    else:
        try:
            with open(topofeed) as fp:
                js = json.load(fp)
                eosc = EOSCReader(js, uidservtype, fetchtype)
                group_groups = eosc.get_groupgroups()
                group_endpoints = eosc.get_groupendpoints()
                state = True
        except IOError as exc:
            logger.error('Customer:%s : Problem opening %s - %s' % (logger.customer, topofeed, repr(exc)))
            state = False

    if fixed_date:
        output.write_state(sys.argv[0], jobstatedir, state,
                           globopts['InputStateDays'.lower()],
                           fixed_date.replace('-', '_'))
    else:
        output.write_state(sys.argv[0], jobstatedir, state,
                           globopts['InputStateDays'.lower()])

    if not state:
        raise SystemExit(1)

    numge = len(group_endpoints)
    numgg = len(group_groups)

    custdir = confcust.get_custdir()
    if eval(globopts['GeneralWriteAvro'.lower()]):
        if fixed_date:
            filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir, fixed_date.replace('-', '_'))
        else:
            filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir)
        avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
        ret, excep = avro.write(group_groups)
        if not ret:
            logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
            raise SystemExit(1)

        if fixed_date:
            filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir, fixed_date.replace('-', '_'))
        else:
            filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir)
        avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename)
        ret, excep = avro.write(group_endpoints)
        if not ret:
            logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
            raise SystemExit(1)

    logger.info('Customer:' + custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (fetchtype, numgg))


if __name__ == '__main__':
    main()
