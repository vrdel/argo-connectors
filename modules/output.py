import datetime
import os

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter, BinaryEncoder

from io import BytesIO

from argo_egi_connectors import helpers
from argo_egi_connectors.log import Logger

from argo_ams_library import AmsMessage, ArgoMessagingService, AmsException


daysback = 1


class AvroWriter:
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
            self.avrofile = open(self.outfile, 'w+')
            self.datawrite = DataFileWriter(self.avrofile, DatumWriter(), lschema)
        except Exception:
            return False

        return True

    def write(self, data):
        try:

            if (not self.datawrite or
                not self.avrofile):
                raise ('AvroFileWriter not initalized')

            for elem in data:
                self.datawrite.append(elem)

            self.datawrite.close()
            self.avrofile.close()

        except Exception as e:
            return False, e

        return True, None


class AmsPublish(object):
    """
       Class represents interaction with AMS service
    """
    def __init__(self, host, project, token, topic, report, bulk, timeout=60):
        self.ams = ArgoMessagingService(host, token, project)
        self.topic = topic
        self.bulk = int(bulk)
        self.report = report
        self.timeout = int(timeout)

    def send(self, schema, msgtype, date, msglist):
        def _avro_serialize(msg):
            opened_schema = load_schema(schema)
            avro_writer = DatumWriter(opened_schema)
            bytesio = BytesIO()
            encoder = BinaryEncoder(bytesio)
            avro_writer.write(msg, encoder)

            return bytesio.getvalue()

        try:
            msgs = map(lambda m: AmsMessage(attributes={'partition_date': date,
                                                        'report': self.report,
                                                        'type': msgtype},
                                            data=_avro_serialize(m)), msglist)
            topic = self.ams.topic(self.topic, timeout=self.timeout)

            if self.bulk > 1:
                q, r = divmod(len(msgs), self.bulk)

                if q:
                    s = 0
                    e = self.bulk - 1

                    for i in range(q):
                        topic.publish(msgs[s:e], timeout=self.timeout)
                        s += self.bulk
                        e += self.bulk
                    topic.publish(msgs[s:], timeout=self.timeout)

                else:
                    topic.publish(msgs, timeout=self.timeout)

            else:
                topic.publish(msgs, timeout=self.timeout)

        except AmsException as e:
            return False, e

        return True, None


def load_schema(schema):
    try:
        f = open(schema)
        schema = avro.schema.parse(f.read())
        return schema
    except Exception as e:
        raise e


def write_state(caller, statedir, state, savedays, datestamp=None):
    filenamenew = ''
    if 'topology' in caller:
        filenamebase = 'topology-ok'
    elif 'poem' in caller:
        filenamebase = 'poem-ok'
    elif 'weights' in caller:
        filenamebase = 'weights-ok'
    elif 'downtimes' in caller:
        filenamebase = 'downtimes-ok'

    if datestamp:
        datebackstamp = datestamp
    else:
        datebackstamp = helpers.datestamp(daysback)

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
