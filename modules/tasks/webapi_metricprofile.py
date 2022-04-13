class TaskWebApiMetricProfile(object):
    def __init__(self, loop, logger, connector_name, globopts, webapi_opts,
                 confcust, custname, feed, DOWNTIMEPI, start, end,
                 uidservtype, targetdate, timestamp):

    async def run(self):
        for job in confcust.get_jobs(cust):
            logger.customer = confcust.get_custname(cust)
            logger.job = job

            profiles = confcust.get_profiles(job)
            webapi_custopts = confcust.get_webapiopts(cust)
            webapi_opts = cglob.merge_opts(webapi_custopts, 'webapi')
            webapi_complete, missopt = cglob.is_complete(webapi_opts, 'webapi')

            if not webapi_complete:
                logger.error('Customer:%s Job:%s %s options incomplete, missing %s' % (custname, logger.job, 'webapi', ' '.join(missopt)))
                continue

            try:
                res = loop.run_until_complete(
                    fetch_data(webapi_opts['webapihost'], webapi_opts['webapitoken'])
                )

                fetched_profiles = parse_source(res, profiles, confcust.get_namespace(job))

                loop.run_until_complete(
                    write_state(cust, job, confcust, fixed_date, True)
                )

                if eval(globopts['GeneralWriteAvro'.lower()]):
                    write_avro(cust, job, confcust, fixed_date, fetched_profiles)

                logger.info('Customer:' + custname + ' Job:' + job + ' Profiles:%s Tuples:%d' % (', '.join(profiles), len(fetched_profiles)))

            except (ConnectorHttpError, KeyboardInterrupt, ConnectorParseError) as exc:
                logger.error(repr(exc))
                loop.run_until_complete(
                    write_state(cust, job, confcust, fixed_date, False)
                )

