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
            tmp_dict['group'] = entity['SITENAME-SERVICEGROUP']
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

    for cust in confcust.get_customers():
        custname = confcust.get_custname(cust)

        for job in confcust.get_jobs(cust):
            jobdir = confcust.get_fulldir(cust, job)
            logger.customer = confcust.get_custname(cust)
            jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust, job)
            fetchtype = confcust.get_fetchtype(job)

            state = None
            logger.job = job
            logger.customer = custname

            uidservtype = confcust.pass_uidserviceendpoints(job)
            ams_custopts = confcust.get_amsopts(cust)
            ams_opts = cglob.merge_opts(ams_custopts, 'ams')
            ams_complete, missopt = cglob.is_complete(ams_opts, 'ams')

            feeds = confcust.get_mapfeedjobs(sys.argv[0])
            if is_feed(list(feeds.keys())[0]):
                remote_topo = urlparse(list(feeds.keys())[0])
                res = input.connection(logger, 'EOSC', globopts, remote_topo.scheme, remote_topo.netloc, remote_topo.path)
                if not res:
                    raise input.ConnectorError()

                doc = input.parse_json(logger, 'EOSC', globopts, res,
                                       remote_topo.scheme + '://' +
                                       remote_topo.netloc + remote_topo.path)
                eosc = EOSCReader(doc, uidservtype, fetchtype)
                group_groups = eosc.get_groupgroups()
                group_endpoints = eosc.get_groupendpoints()
                state = True
            else:
                try:
                    with open(list(feeds.keys())[0]) as fp:
                        js = json.load(fp)
                        eosc = EOSCReader(js, uidservtype, fetchtype)
                        group_groups = eosc.get_groupgroups()
                        group_endpoints = eosc.get_groupendpoints()
                        state = True
                except IOError as exc:
                    logger.error('Customer:%s Job:%s : Problem opening %s - %s' % (logger.customer, logger.job, feeds.keys()[0], repr(exc)))
                    state = False

            if fixed_date:
                output.write_state(sys.argv[0], jobstatedir, state,
                                   globopts['InputStateDays'.lower()],
                                   fixed_date.replace('-', '_'))
            else:
                output.write_state(sys.argv[0], jobstatedir, state,
                                   globopts['InputStateDays'.lower()])

            if not state:
                continue

            numge = len(group_endpoints)
            numgg = len(group_groups)

            if eval(globopts['GeneralPublishAms'.lower()]):
                if fixed_date:
                    partdate = fixed_date
                else:
                    partdate = datestamp(1).replace('_', '-')

                ams = output.AmsPublish(ams_opts['amshost'],
                                        ams_opts['amsproject'],
                                        ams_opts['amstoken'],
                                        ams_opts['amstopic'],
                                        confcust.get_jobdir(job),
                                        ams_opts['amsbulk'],
                                        ams_opts['amspacksinglemsg'],
                                        logger,
                                        int(globopts['ConnectionRetry'.lower()]),
                                        int(globopts['ConnectionTimeout'.lower()]))

                ams.send(globopts['AvroSchemasTopologyGroupOfGroups'.lower()],
                         'group_groups', partdate, group_groups)

                ams.send(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()],
                         'group_endpoints', partdate, group_endpoints)

            if eval(globopts['GeneralWriteAvro'.lower()]):
                if fixed_date:
                    filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], jobdir, fixed_date.replace('-', '_'))
                else:
                    filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
                import ipdb; ipdb.set_trace()
                ret, excep = avro.write(group_groups)
                if not ret:
                    logger.error('Customer:%s Job:%s : %s' % (logger.customer, logger.job, repr(excep)))
                    raise SystemExit(1)

                if fixed_date:
                    filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], jobdir, fixed_date.replace('-', '_'))
                else:
                    filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], jobdir)
                avro = output.AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename)
                ret, excep = avro.write(group_endpoints)
                if not ret:
                    logger.error('Customer:%s Job:%s : %s' % (logger.customer, logger.job, repr(excep)))
                    raise SystemExit(1)

            logger.info('Customer:' + custname + ' Job:' + job + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (fetchtype, numgg))


if __name__ == '__main__':
    main()
