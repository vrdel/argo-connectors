import os

from argo_connectors.config.customer import Customer
from argo_connectors.config.glob import Global
from argo_connectors.exceptions import ConnectorHttpError, ConnectorParseError
from argo_connectors.io.http import SessionWithRetry
from argo_connectors.tasks.common import write_weights_metricprofile_state as write_state, write_metricprofile_json as write_json
from argo_connectors.parse.webapi_metricprofile import ParseMetricProfiles

API_PATH = '/api/v2/metric_profiles'


class TaskWebApiMetricProfile(object):
    def __init__(self, logger, cust, fixed_date):
        self.logger = logger
        self.connector_name = Global.caller
        self.globopts = Global.options()
        self.cust = cust
        self.fixed_date = fixed_date

    async def fetch_data(self, host, token):
        session = SessionWithRetry(self.logger,
                                   os.path.basename(self.connector_name),
                                   self.globopts, token=token)
        res = await session.http_get('{}://{}{}'.format('https', host, API_PATH))
        return res

    def parse_source(self, res, profiles):
        metric_profiles = ParseMetricProfiles(self.logger, res, profiles).get_data()
        return metric_profiles

    async def run(self):
        for job in Customer.get_jobs(self.cust):
            self.logger.customer = Customer.get_custname(self.cust)
            self.logger.job = job

            profiles = Customer.get_profiles(job)
            webapi_opts = Customer.webapi_opts.opts

            try:
                res = await self.fetch_data(webapi_opts['webapihost'], webapi_opts['webapitoken'])

                fetched_profiles = self.parse_source(res, profiles)

                await write_state(self.cust, job, self.fixed_date, True)

                if self.globopts['GeneralWriteJson'.lower()]:
                    write_json(self.logger, self.cust, job, self.fixed_date, fetched_profiles)

                self.logger.info('Customer:' + self.logger.customer + ' Job:' + job + ' Profiles:%s Tuples:%d' % (', '.join(profiles), len(fetched_profiles)))

            except (ConnectorHttpError, KeyboardInterrupt, ConnectorParseError) as exc:
                self.logger.error(repr(exc))
                await write_state(self.cust, job, self.fixed_date, False)
