import datetime
import os
import json
import requests

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter, BinaryEncoder

from io import BytesIO

from argo_egi_connectors.helpers import datestamp, retry, module_class_name


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
        'topology-gocdb-connector.py': 'topology',
        'topology-eosc-connector.py': 'topology',
        'weights-vapor-connector.py': 'weights'
    }

    def __init__(self, connector, host, token, logger, retry,
                 timeout=180, sleepretry=60, report=None, endpoints_group=None,
                 date=None, verifycert=False):
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
        self.date = date or self._construct_datenow()
        self.verifycert = eval(verifycert)

    def _construct_datenow(self):
        d = datetime.datetime.now()

        return d.strftime('%Y-%m-%d')

    def _format_downtimes(self, data):
        formatted = dict()

        formatted['endpoints'] = data

        return formatted

    def _format_weights(self, data):
        formatted = dict()

        if data:
            formatted['weight_type'] = data[0]['type']
            groups = map(lambda s: {'name': s['site'], 'value': float(s['weight'])}, data)
        else:
            formatted['weight_type'] = ''
            groups = []
        formatted['name'] = self.report
        formatted['groups'] = list(groups)
        formatted['name'] = self.report
        formatted['group_type'] = self.endpoints_group

        return formatted

    @staticmethod
    @retry
    def _send(logger, msgprefix, retryopts, api, data_send, headers, connector,
              verifycert=False):
        ret = requests.post(api, data=json.dumps(data_send), headers=headers,
                            timeout=retryopts['ConnectionTimeout'.lower()],
                            verify=verifycert)
        if ret.status_code != 201:
            if connector.startswith('topology') or connector.startswith('downtimes'):
                logger.error('%s %s() Customer:%s - HTTP POST %s' % (msgprefix,
                                                                     '_send',
                                                                     logger.customer,
                                                                     ret.content))
            else:
                logger.error('%s %s() Customer:%s Job:%s - HTTP POST %s' %
                             (msgprefix, '_send', logger.customer, logger.job,
                              ret.content))
        return ret.status_code

    @staticmethod
    @retry
    def _get(logger, msgprefix, retryopts, api, headers, verifycert=False):
        ret = requests.get(api, headers=headers,
                           timeout=retryopts['ConnectionTimeout'.lower()], verify=verifycert)
        return json.loads(ret.content)

    @staticmethod
    @retry
    def _delete(logger, msgprefix, retryopts, api, headers, id=None, verifycert=False):
        from urllib.parse import urlparse
        loc = urlparse(api)
        if id is not None:
            loc = '{}://{}{}/{}'.format(loc.scheme, loc.hostname, loc.path, id)
        else:
            loc = '{}://{}{}'.format(loc.scheme, loc.hostname, loc.path)
        ret = requests.delete(loc, headers=headers,
                              timeout=retryopts['ConnectionTimeout'.lower()], verify=verifycert)
        return ret

    @staticmethod
    @retry
    def _put(logger, msgprefix, retryopts, api, data_send, id, headers, verifycert=False):
        from urllib.parse import urlparse
        loc = urlparse(api)
        loc = '{}://{}{}/{}?{}'.format(loc.scheme, loc.hostname, loc.path, id, loc.query)
        ret = requests.put(loc, data=json.dumps(data_send), headers=headers,
                           timeout=retryopts['ConnectionTimeout'.lower()],
                           verify=verifycert)
        return ret

    def _update(self, api, data_send):
        ret = self._get(self.logger, module_class_name(self),
                        self.retry_options, api, self.headers, self.verifycert)
        target_report = filter(lambda w: w['name'] == data_send['name'], ret['data'])
        id = list(target_report)[0]['id']
        ret = self._put(self.logger, module_class_name(self),
                        self.retry_options, api, data_send, id, self.headers,
                        self.verifycert)
        if ret.status_code == 200:
            self.logger.info('Succesfully updated (HTTP PUT) resource')
        else:
            self.logger.error('%s %s() Customer:%s Job:%s - HTTP PUT %s' %
                              (module_class_name(self), '_update',
                               self.logger.customer, self.logger.job,
                               ret.content))

    def _delete_and_resend(self, api, data_send, topo_component, downtimes_component):
        id = None
        data = self._get(self.logger, module_class_name(self),
                         self.retry_options, api, self.headers,
                         self.verifycert)
        if not topo_component and not downtimes_component:
            id = data['data'][0]['id']
        ret = self._delete(self.logger, module_class_name(self),
                           self.retry_options, api, self.headers, id,
                           self.verifycert)
        if ret.status_code == 200:
            self._send(self.logger, module_class_name(self),
                       self.retry_options, api, data_send, self.headers,
                       self.connector, self.verifycert)
            self.logger.info('Succesfully deleted and created new resource')

    def send(self, data, topo_component=None, downtimes_component=None):
        if topo_component:
            # /topology/groups, /topology/endpoints
            webapi_url = '{}/{}'.format(self.webapi_method, topo_component)
        else:
            webapi_url = self.webapi_method

        if self.date:
            api = 'https://{}/api/v2/{}?date={}'.format(self.host,
                                                        webapi_url,
                                                        self.date)
        else:
            api = 'https://{}/api/v2/{}'.format(self.host, webapi_url)

        if topo_component:
            data_send = data
        else:
            data_send = dict()

        if self.connector.startswith('downtimes'):
            data_send = self._format_downtimes(data)

        if self.connector.startswith('weights'):
            data_send = self._format_weights(data)

        ret = self._send(self.logger, module_class_name(self),
                         self.retry_options, api, data_send, self.headers,
                         self.connector, self.verifycert)

        # delete resource on WEB-API and resend
        if ret == 409 and topo_component or downtimes_component:
            self._delete_and_resend(api, data_send, topo_component, downtimes_component)
        elif ret == 409:
            self._update(api, data_send)


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
    while i < int(savedays) * 2:
        d = datestart - datetime.timedelta(days=i)
        filenameold = filenamebase + '_' + d.strftime('%Y_%m_%d')
        if os.path.exists(statedir + '/' + filenameold):
            os.remove(statedir + '/' + filenameold)
        i += 1

    with open(statedir + '/' + filenamenew, 'w') as fp:
        fp.write(str(state))
