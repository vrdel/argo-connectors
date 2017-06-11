import datetime
import os

import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter

from argo_egi_connectors import helpers
from argo_egi_connectors.log import Logger

daysback = 1

class AvroWriter:
    """ AvroWriter """
    def __init__(self, schema, outfile, listdata, name):
        self.logger = Logger(name)
        self.schema = schema
        self.listdata = listdata
        self.outfile = outfile

    def write(self):
        try:
            schema = avro.schema.parse(open(self.schema).read())
            avrofile = open(self.outfile, 'w+')
            datawrite = DataFileWriter(avrofile, DatumWriter(), schema)

            for elem in self.listdata:
                datawrite.append(elem)

            datawrite.close()
            avrofile.close()

        except (avro.schema.SchemaParseException, avro.io.AvroTypeException):
            self.logger.error(" couldn't parse %s" % self.schema)
            raise SystemExit(1)
        except IOError as e:
            self.logger.error(e)
            raise SystemExit(1)

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
        datebackstamp = helpers.gen_fname_timestamp(daysback)

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
