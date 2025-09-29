import os

from urllib.parse import urlparse

from argo_connectors.config.glob import Global
from argo_connectors.config.customer import get_custconf
from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.parse.flat_downtimes import ParseDowntimes
from argo_connectors.tasks.common import write_state, write_downtimes_json as write_json
from argo_connectors.utils import module_class_name


class TaskCsvDowntimes(object):
    def __init__(self, logger, current_date, targetdate, timestamp, combuid=None):
        self.logger = logger
        self.current_date = current_date
        self.Customer = get_custconf(combuid)
        self.feed = self.Customer.opt('DowntimesFeed')
        self.targetdate = targetdate
        self.timestamp = timestamp
        self.connector_name = Global.caller
        self.globopts = Global.options()
        self.timestamp = timestamp
        self.combuid = combuid

    async def fetch_data(self):
        session = SessionWithRetry(self.logger,
                                   os.path.basename(self.connector_name),
                                   self.globopts)
        res = await session.http_get(self.feed)

        return res

    def parse_source(self, res):
        csv_downtimes = ParseDowntimes(self.logger, res, self.current_date)
        return csv_downtimes.get_data()

    async def run(self):
        try:
            write_empty = self.Customer.send_empty(self.connector_name)
            if not write_empty:
                res = await self.fetch_data()
                dts = self.parse_source(res)
            else:
                dts = []

            if not self.combuid:
                await write_state(self.timestamp, True)

            if not self.combuid:
                if eval(self.globopts['GeneralPublishWebAPI'.lower()]):
                    webapi = WebAPI(self.logger, date=self.targetdate, combuid=self.combuid)
                    await webapi.send(dts, downtimes_component=True)
                    await webapi.session.close()

                if eval(self.globopts['GeneralWriteJson'.lower()]):
                    write_json(self.logger, dts, self.timestamp)

            # we don't have multiple tenant definitions in one
            # customer file so we can safely assume one tenant/customer
            if dts or write_empty:
                cust = list(self.Customer.get_customers())[0]
                if not self.combuid:
                    self.logger.info('Customer:%s Fetched Date:%s Endpoints:%d' %
                                     (self.Customer.get_custname(cust), self.targetdate, len(dts)))
                else:
                    self.logger.info(module_class_name(self) + 'ID:' + self.combuid + ' Customer:%s Fetched Date:%s Endpoints:%d' %
                                     (self.Customer.get_custname(cust), self.targetdate, len(dts)))

        except (ConnectorHttpError, ConnectorParseError, KeyboardInterrupt) as exc:
            self.logger.error(repr(exc))
            await write_state(self.timestamp, False)
