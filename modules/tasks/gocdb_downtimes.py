import os

from urllib.parse import urlparse

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import get_custconf
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.parse.gocdb_downtimes import ParseDowntimes
from argo_connectors.io.webapi import WebAPI
from argo_connectors.tasks.common import write_state, write_downtimes_json as write_json


class TaskGocdbDowntimes(object):
    def __init__(self, logger, start, end, targetdate, timestamp,
                 combuid=None):
        self.logger = logger
        self.globopts = Global.options()
        self.connector_name = Global.caller
        self.Customer = get_custconf(combuid)
        self.globopts = Global.options()
        self.auth_opts = self.Customer.auth_opts.opts
        self.webapi_opts = self.Customer.webapi_opts.opts
        self.custname = self.Customer.get_custname()
        downtime_feed = self.Customer.opt('DowntimesFeed')
        toposcope = self.Customer.opt('TopoScope')
        if toposcope and '&scope=' not in toposcope:
            downtime_feed += '&scope={}'.format(toposcope)
        elif toposcope and '&scope=' in toposcope:
            downtime_feed += toposcope
        self.feed = downtime_feed
        self.start = start
        self.end = end
        self.targetdate = targetdate
        self.timestamp = timestamp
        self.combuid = combuid

    async def fetch_data(self):
        feed_parts = urlparse(self.feed)
        start_fmt = self.start.strftime("%Y-%m-%d")
        end_fmt = self.end.strftime("%Y-%m-%d")
        session = SessionWithRetry(self.logger,
                                   os.path.basename(self.connector_name),
                                   self.globopts,
                                   custauth=self.auth_opts)
        if feed_parts.query:
            query_url = \
            '{}://{}{}?{}&windowstart={}&windowend={}'.format(feed_parts.scheme,
                                                              feed_parts.netloc,
                                                              feed_parts.path,
                                                              feed_parts.query,
                                                              start_fmt,
                                                              end_fmt)
        else:
            query_url = \
            '{}://{}{}?windowstart={}&windowend={}'.format(feed_parts.scheme,
                                                           feed_parts.netloc,
                                                           feed_parts.path,
                                                           start_fmt, end_fmt)
        res = await session.http_get(query_url)

        return res

    def parse_source(self, res):
        gocdb = ParseDowntimes(self.logger, res, self.start, self.end, self.combuid)
        return gocdb.get_data()

    async def send_webapi(self, dts):
        webapi = WebAPI(self.connector_name, self.webapi_opts['webapihost'],
                        self.webapi_opts['webapitoken'], self.logger,
                        int(self.globopts['ConnectionRetry'.lower()]),
                        int(self.globopts['ConnectionTimeout'.lower()]),
                        int(self.globopts['ConnectionSleepRetry'.lower()]),
                        self.globopts['ConnectionRetryRandom'.lower()],
                        int(self.globopts['ConnectionSleepRandomRetryMax'.lower()]),
                        date=self.targetdate)
        await webapi.send(dts, downtimes_component=True)

    async def run(self):
        # we don't have multiple tenant definitions in one
        # customer file so we can safely assume one tenant/customer
        write_empty = self.Customer.send_empty(self.connector_name)
        if not write_empty:
            res = await self.fetch_data()
            dts = self.parse_source(res)
        else:
            dts = []

        await write_state(self.timestamp, True)

        if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
            await self.send_webapi(dts)

        if dts or write_empty:
            cust = list(self.Customer.get_customers())[0]
            self.logger.info('Customer:%s Fetched Date:%s Endpoints:%d' %
                             (self.Customer.get_custname(cust),
                              self.targetdate, len(dts)))

        if eval(self.globopts['GeneralWriteJson'.lower()]):
            write_json(self.logger, dts, self.timestamp)
