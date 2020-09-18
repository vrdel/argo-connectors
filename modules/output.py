import datetime
import os
import json
import requests

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter, BinaryEncoder

from io import BytesIO

from argo_egi_connectors.helpers import datestamp, retry, module_class_name

from argo_ams_library import AmsMessage, ArgoMessagingService, AmsException


daysback = 1


class AvroWriteException(BaseException):
    pass


class AvroWriter(object):
    """ AvroWriter """
    def __init__(self, schema, outfile):
        self.schema = schema
        self.outfile = outfile
        self.datawrite = None
        self.avrofile = None
        self._load_datawriter()

    def _load_datawriter(self):
        try:
            lschema = load_schema(self.schema)
            self.avrofile = open(self.outfile, 'w+b')
            self.datawrite = DataFileWriter(self.avrofile, DatumWriter(), lschema)
        except Exception:
            return False

        return True

    def write(self, data):
        try:
            if (not self.datawrite or not self.avrofile):
                raise AvroWriteException('AvroFileWriter not initalized')

            for elem in data:
                self.datawrite.append(elem)

            self.datawrite.close()
            self.avrofile.close()

        except Exception as e:
            return False, e

        return True, None


class WebAPI(object):
    methods = {
        'downtimes-gocdb-connector.py': 'downtimes',
        'topology-gocdb-connector.py': ['topology/endpoints', 'topology/groups'],
        'topology-eosc-connector.py': ['topology/endpoints', 'topology/groups'],
        'weights-vapor-connector.py': 'weights'
    }

    def __init__(self, connector, host, token, report, logger, retry,
                 timeout=180, sleepretry=60, endpoints_group=None, date=None):
        self.connector = os.path.basename(connector)
        self.webapi_method = self.methods[self.connector]
        self.host = host
        self.token = token
        self.headers = {
            'x-api-key': self.token,
            'Accept': 'application/json'
        }
        self.report = report
        self.logger = logger
        self.retry = retry
        self.timeout = timeout
        self.sleepretry = sleepretry
        self.retry_options = {
            'ConnectionRetry'.lower(): retry,
            'ConnectionTimeout'.lower(): timeout,
            'ConnectionSleepRetry'.lower(): sleepretry
        }
        self.endpoints_group = endpoints_group
        self.date = date

    def _format_downtimes(self, data):
        formatted = dict()

        formatted['endpoints'] = data
        formatted['name'] = self.report

        return formatted

    def _format_weights(self, data):
        formatted = dict()

        formatted['weight_type'] = data[0]['type']
        formatted['name'] = self.report
        groups = map(lambda s: {'name': s['site'], 'value': float(s['weight'])}, data)
        formatted['groups'] = list(groups)
        formatted['name'] = self.report
        formatted['group_type'] = self.endpoints_group

        return formatted

    @staticmethod
    @retry
    def _send(logger, msgprefix, retryopts, api, data_send, headers):
        ret = requests.post(api, data=json.dumps(data_send), headers=headers,
                            timeout=retryopts['ConnectionTimeout'.lower()])
        if ret.status_code != 201:
            logger.error('%s %s() Customer:%s Job:%s - %s' % (msgprefix,
                                                              '_send',
                                                              logger.customer,
                                                              logger.job,
                                                              ret.content))

    def send(self, data):
        if self.date:
            api = 'https://{}/api/v2/{}?date={}'.format(self.host,
                                                        self.webapi_method,
                                                        self.date)
        else:
            api = 'https://{}/api/v2/{}'.format(self.host, self.webapi_method)

        data_send = dict()

        if self.connector.startswith('downtimes'):
            data_send = self._format_downtimes(data)

        if self.connector.startswith('weights'):
            data_send = self._format_weights(data)

        self._send(self.logger, module_class_name(self), self.retry_options,
                   api, data_send, self.headers)


class AmsPublish(object):
    """
       Class represents interaction with AMS service
    """
    def __init__(self, host, project, token, topic, report, bulk, packsingle,
                 logger, retry, timeout=180, sleepretry=60):
        self.ams = ArgoMessagingService(host, token, project)
        self.topic = topic
        self.bulk = int(bulk)
        self.report = report
        self.timeout = int(timeout)
        self.retry = int(retry)
        self.sleepretry = int(sleepretry)
        self.logger = logger
        self.packsingle = eval(packsingle)

    @staticmethod
    @retry
    def _send(logger, msgprefix, retryopts, msgs, bulk, obj):
        timeout = retryopts['ConnectionTimeout'.lower()]
        msgs = list(msgs)
        try:
            if bulk > 1:
                q, r = divmod(len(msgs), bulk)

                if q:
                    s = 0
                    e = bulk - 1

                    for i in range(q):
                        obj.ams.publish(obj.topic, msgs[s:e], timeout=timeout)
                        s += bulk
                        e += bulk
                    obj.ams.publish(obj.topic, msgs[s:], timeout=timeout)

                else:
                    obj.ams.publish(obj.topic, msgs, timeout=timeout)

            else:
                obj.ams.publish(obj.topic, msgs, timeout=timeout)

        except AmsException as e:
            raise e

        return True

    def send(self, schema, msgtype, date, msglist):
        def _avro_serialize(msg):
            opened_schema = load_schema(schema)
            avro_writer = DatumWriter(opened_schema)
            bytesio = BytesIO()
            encoder = BinaryEncoder(bytesio)
            if isinstance(msg, list):
                for m in msg:
                    avro_writer.write(m, encoder)
            else:
                avro_writer.write(msg, encoder)

            return bytesio.getvalue()

        if self.packsingle:
            self.bulk = 1
            msg = AmsMessage(attributes={'partition_date': date,
                                         'report': self.report,
                                         'type': msgtype},
                             data=_avro_serialize(msglist))
            msgs = [msg]

        else:
            msgs = map(lambda m: AmsMessage(attributes={'partition_date': date,
                                                        'report': self.report,
                                                        'type': msgtype},
                                            data=_avro_serialize(m)), msglist)

        if self._send(self.logger, module_class_name(self),
                      {'ConnectionRetry'.lower(): self.retry,
                       'ConnectionTimeout'.lower(): self.timeout,
                       'ConnectionSleepRetry'.lower(): self.sleepretry}, msgs, self.bulk, self):
            return True


def load_schema(schema):
    try:
        f = open(schema)
        schema = avro.schema.parse(f.read())
        return schema
    except Exception as e:
        raise e


def write_state(caller, statedir, state, savedays, date=None):
    filenamenew = ''
    if 'topology' in caller:
        filenamebase = 'topology-ok'
    elif 'metricprofile' in caller:
        filenamebase = 'metricprofile-ok'
    elif 'weights' in caller:
        filenamebase = 'weights-ok'
    elif 'downtimes' in caller:
        filenamebase = 'downtimes-ok'

    if date:
        datebackstamp = date
    else:
        datebackstamp = datestamp(daysback)

    filenamenew = filenamebase + '_' + datebackstamp
    db = datetime.datetime.strptime(datebackstamp, '%Y_%m_%d')

    datestart = db - datetime.timedelta(days=int(savedays))
    i = 0
    while i < int(savedays)*2:
        d = datestart - datetime.timedelta(days=i)
        filenameold = filenamebase + '_' + d.strftime('%Y_%m_%d')
        if os.path.exists(statedir + '/' + filenameold):
            os.remove(statedir + '/' + filenameold)
        i += 1

    with open(statedir + '/' + filenamenew, 'w') as fp:
        fp.write(str(state))
