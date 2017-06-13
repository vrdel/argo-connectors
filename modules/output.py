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

    def write(self, data):
        try:
            schema = load_schema(self.schema)
            avrofile = open(self.outfile, 'w+')
            datawrite = DataFileWriter(avrofile, DatumWriter(), schema)

            for elem in data:
                datawrite.append(elem)

            datawrite.close()
            avrofile.close()

        except Exception as e:
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
