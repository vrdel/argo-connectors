import os

from urllib.parse import urlparse

from argo_egi_connectors.io.http import SessionWithRetry
from argo_egi_connectors.parse.gocdb_downtimes import ParseDowntimes
from argo_egi_connectors.io.webapi import WebAPI
from argo_egi_connectors.tasks.common import write_state, write_downtimes_avro as write_avro


class TaskGocdbServiceTypes(object):
    def __init__(self, loop, logger, connector_name, globopts, webapi_opts, confcust,
                 custname, feed):
        self.logger = logger
        self.loop = loop
        self.connector_name = connector_name
        self.globopts = globopts
        self.webapi_opts = webapi_opts
        self.confcust = confcust
        self.custname = custname
        self.feed = feed

    async def fetch_data(self):
        feed_parts = urlparse(self.feed)
        session = SessionWithRetry(self.logger,
                                   os.path.basename(self.connector_name),
                                   self.globopts)
        res = await session.http_get('{}://{}{}?{}'.format(feed_parts.scheme,
                                                         feed_parts.netloc,
                                                         feed_parts.path,
                                                         feed_parts.query))

        return res

    async def run(self):
        res = await self.fetch_data()
