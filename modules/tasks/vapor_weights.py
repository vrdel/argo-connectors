import os

from urllib.parse import urlparse

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.parse.vapor import ParseWeights
from argo_egi_connectors.tasks.common import write_state, write_weights_avro as write_avro


class TaskVaporWeights(object):
    def __init__(self, loop, logger, connector_name, globopts, webapi_opts,
                 confcust, feed, jobcust, fixed_date):
        self.event_loop = loop
        self.logger = logger
        self.connector_name = connector_name
        self.globopts = globopts
        self.webapi_opts = webapi_opts
        self.confcust = confcust
        self.feed = feed
        self.jobcust = jobcust
        self.fixed_date = fixed_date

    async def fetch_data(self):
        feed_parts = urlparse(self.feed)
        session = SessionWithRetry(self.logger, os.path.basename(self.connector_name), self.globopts)
        res = await session.http_get('{}://{}{}'.format(feed_parts.scheme,
                                                        feed_parts.netloc,
                                                        feed_parts.path))
        return res

    def parse_source(self, res):
        weights = ParseWeights(self.logger, res).get_data()
        return weights

    async def send_webapi(self, weights, job):
        webapi = WebAPI(self.connector_name, self.webapi_opts['webapihost'],
                        self.webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        report=self.confcust.get_jobdir(job), endpoints_group='SITES',
                        date=self.fixed_date)
        await webapi.send(weights)

    async def run(self):
        for job, cust in self.jobcust:
            self.logger.customer = self.confcust.get_custname(cust)
            self.logger.job = job

            write_empty = self.confcust.send_empty(self.connector_name, cust)

            if write_empty:
                weights = []
            else:
                res = await self.fetch_data()
                weights = self.parse_source(res)

            if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
                await self.send_webapi(weights, job)

            if eval(self.globopts['GeneralWriteAvro'.lower()]):
                write_avro(self.logger, self.globopts, cust, job,
                           self.confcust, self.fixed_date, weights)

            await write_state(cust, job, self.confcust, self.fixed_date, True)

            if weights or write_empty:
                custs = set([cust for job, cust in self.jobcust])
                for cust in custs:
                    jobs = [job for job, lcust in self.jobcust if cust == lcust]
                    self.logger.info('Customer:%s Jobs:%s Sites:%d' %
                                     (self.confcust.get_custname(cust), jobs[0]
                                      if len(jobs) == 1 else
                                      '({0})'.format(','.join(jobs)),
                                      len(weights)))
