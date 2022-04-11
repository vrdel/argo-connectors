from argo_egi_connectors.io.statewrite import state_write
from argo_egi_connectors.utils import filename_date, datestamp, date_check
from argo_egi_connectors.io.avrowrite import AvroWriter


async def write_state(connector_name, globopts, confcust, fixed_date, state):
    cust = list(confcust.get_customers())[0]
    jobstatedir = confcust.get_fullstatedir(globopts['InputStateSaveDir'.lower()], cust)
    fetchtype = confcust.get_topofetchtype()
    if fixed_date:
        await state_write(connector_name, jobstatedir, state,
                          globopts['InputStateDays'.lower()],
                          fixed_date.replace('-', '_'))
    else:
        await state_write(connector_name, jobstatedir, state,
                          globopts['InputStateDays'.lower()])


def write_avro(logger, globopts, confcust, group_groups, group_endpoints, fixed_date):
    custdir = confcust.get_custdir()
    if fixed_date:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfGroups'.lower()], custdir)
    avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfGroups'.lower()], filename)
    ret, excep = avro.write(group_groups)
    if not ret:
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)

    if fixed_date:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir, fixed_date.replace('-', '_'))
    else:
        filename = filename_date(logger, globopts['OutputTopologyGroupOfEndpoints'.lower()], custdir)
    avro = AvroWriter(globopts['AvroSchemasTopologyGroupOfEndpoints'.lower()], filename)
    ret, excep = avro.write(group_endpoints)
    if not ret:
        logger.error('Customer:%s : %s' % (logger.customer, repr(excep)))
        raise SystemExit(1)
