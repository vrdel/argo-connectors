import json
import asyncio

from urllib.parse import urlparse

from argo_connectors.config.customer import get_custconf
from argo_connectors.config.glob import Global
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.io.webapi import WebAPI
from argo_connectors.mesh.contacts import attach_contacts_topodata
from argo_connectors.parse.flat_contacts import ParseContacts
from argo_connectors.parse.flat_topology import ParseFlatEndpoints
from argo_connectors.tasks.common import write_state, write_topo_json as write_json
from argo_connectors.utils import module_class_name


class TaskFlatTopology(object):
    def __init__(self, logger, fixed_date, is_csv=False, combuid=None):
        self.logger = logger
        self.connector_name = Global.caller
        self.globopts = Global.options()
        self.Customer = get_custconf(combuid)
        self.custname = self.Customer.get_custname()
        self.topofeed = self.Customer.opt('TopoFeed')
        self.topofetchtype = self.Customer.get_topofetchtype()[0]
        self.fixed_date = fixed_date
        self.uidservendp = self.Customer.opt('TopoUIDServiceEndpoints')
        self.is_csv = is_csv
        self.combuid = combuid

    def _is_feed(self, feed):
        data = urlparse(feed)

        if not data.netloc:
            return False
        else:
            return True

    async def fetch_data(self):
        remote_topo = urlparse(self.topofeed)
        session = SessionWithRetry(self.custname, self.globopts)
        if remote_topo.query:
            res = await \
                session.http_get('{}://{}{}?{}'.format(remote_topo.scheme,
                                                       remote_topo.netloc,
                                                       remote_topo.path,
                                                       remote_topo.query))
        else:
            res = await session.http_get('{}://{}{}'.format(remote_topo.scheme,
                                                            remote_topo.netloc,
                                                            remote_topo.path))
        return res

    def parse_source_topo(self, res):
        topo = ParseFlatEndpoints(self.logger, res, self.custname,
                                  self.uidservendp, self.topofetchtype,
                                  self.is_csv, scope=self.custname)
        group_groups = topo.get_groupgroups()
        group_endpoints = topo.get_groupendpoints()

        return group_groups, group_endpoints

    async def run(self):
        if self._is_feed(self.topofeed):
            res = await self.fetch_data()
            group_groups, group_endpoints = self.parse_source_topo(res)
            contacts = ParseContacts(self.logger, res, self.uidservendp, self.is_csv).get_contacts()
            attach_contacts_topodata(self.logger, contacts, group_endpoints)

        elif not self._is_feed(self.topofeed) and not self.is_csv:
            try:
                with open(self.topofeed) as fp:
                    js = json.load(fp)
                    group_groups, group_endpoints = self.parse_source_topo(js)
            except IOError as exc:
                self.logger.error('Customer:%s : Problem opening %s - %s' % (self.logger.customer, self.topofeed, repr(exc)))

        if not self.combuid:
            await write_state(self.fixed_date, True)

        numge = len(group_endpoints)
        numgg = len(group_groups)

        if not self.combuid:
            # send concurrently to WEB-API in coroutines
            if self.globopts['GeneralPublishWebAPI'.lower()]:
                webapi = WebAPI(self.logger, date=self.fixed_date, combuid=self.combuid)
                await asyncio.gather(
                    webapi.send(group_groups, 'groups'),
                    webapi.send(group_endpoints, 'endpoints')
                )
                await webapi.session.close()

            if self.globopts['GeneralWriteJson'.lower()]:
                write_json(self.logger, group_groups, group_endpoints, self.fixed_date)

        if not self.combuid:
            self.logger.info('Customer:' + self.custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (self.topofetchtype, numgg))
        else:
            self.logger.info(module_class_name(self) + ' ID:' + self.combuid + ' Customer:' + self.custname + ' Fetched Endpoints:%d' % (numge) + ' Groups(%s):%d' % (self.topofetchtype, numgg))

            return group_groups, group_endpoints
