from argo_connectors.io.statewrite import state_write
from argo_connectors.utils import filename_date, datestamp, date_check
from argo_connectors.io.jsonwrite import JsonWriter
from argo_connectors.config.glob import Global
from argo_connectors.config.customer import Customer


async def write_state(fixed_date, state):
    # TODO: ditch confcust arg
    cust = list(Customer.get_customers())[0]
    jobstatedir = Customer.get_fullstatedir(
        Global.options()['InputStateSaveDir'.lower()], cust)
    if fixed_date:
        await state_write(jobstatedir, state, fixed_date.replace('-', '_'))
    else:
        await state_write(jobstatedir, state)


async def write_weights_metricprofile_state(connector_name, globopts, cust, job, confcust, fixed_date, state):
    jobstatedir = confcust.get_fullstatedir(
        globopts['InputStateSaveDir'.lower()], cust, job)
    if fixed_date:
        await state_write(connector_name, jobstatedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(connector_name, jobstatedir, state,
                          globopts['InputStateDays'.lower()])


def write_metricprofile_json(logger, globopts, cust, job, confcust, fixed_date, fetched_profiles):
    jobdir = confcust.get_fulldir(cust, job)
    if fixed_date:
        filename = filename_date(logger, globopts['OutputMetricProfile'.lower(
        )], jobdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(
            logger, globopts['OutputMetricProfile'.lower()], jobdir)
    json_writer = JsonWriter(fetched_profiles, filename, globopts['generalcompressjson'])
    ret, excep = json_writer.write_json()
    if not ret:
        logger.error('Customer:%s Job:%s %s' %
                     (logger.customer, logger.job, repr(excep)))
        raise SystemExit(1)


def write_downtimes_json(logger, dts, timestamp):
    custdir = Customer.get_custdir()
    filename = filename_date(
        logger, Global.options()['OutputDowntimes'.lower()], custdir, stamp=timestamp)
    json_writer = JsonWriter(dts, filename, Global.options()['generalcompressjson'])
    ret, excep = json_writer.write_json()
    if not ret:
        logger.error('Customer:{} {}'.format(logger.customer, repr(excep)))
        raise SystemExit(1)


def write_weights_json(logger, globopts, cust, job, confcust, fixed_date, weights):
    jobdir = confcust.get_fulldir(cust, job)
    if fixed_date:
        filename = filename_date(
            logger, globopts['OutputWeights'.lower()], jobdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(
            logger, globopts['OutputWeights'.lower()], jobdir)

    json_writer = JsonWriter(weights, filename, globopts['generalcompressjson'])
    ret, excep = json_writer.write_json()
    if not ret:
        logger.error('Customer:%s Job:%s %s' %
                     (logger.customer, logger.job, repr(excep)))
        raise SystemExit(1)


def write_topo_json(logger, group_groups, group_endpoints, fixed_date):
    custdir = Customer.get_custdir()
    if fixed_date:
        filename = filename_date(logger, Global.options()['OutputTopologyGroupOfGroups'.lower(
        )], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(
            logger, Global.options()['OutputTopologyGroupOfGroups'.lower()], custdir)
    json_writer = JsonWriter(group_groups, filename, Global.options()['generalcompressjson'])
    ret, excep = json_writer.write_json()
    if not ret:
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)

    if fixed_date:
        filename = filename_date(logger, Global.options()['OutputTopologyGroupOfEndpoints'.lower(
        )], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(
            logger, Global.options()['OutputTopologyGroupOfEndpoints'.lower()], custdir)
    json_writer = JsonWriter(group_endpoints, filename, Global.options()['generalcompressjson'])
    ret, excep = json_writer.write_json()
    if not ret:
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)
