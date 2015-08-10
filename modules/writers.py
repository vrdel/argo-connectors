import avro.schema
from avro.datafile import DataFileWriter
from avro.io import DatumWriter
import logging, logging.handlers
import sys
from exceptions import IOError

class Logger:
    def __init__(self, connector):
        lfs = '%(name)s[%(process)s]: %(levelname)s %(message)s'
        lf = logging.Formatter(lfs)
        lv = logging.INFO

        logging.basicConfig(format=lfs, level=logging.INFO, stream=sys.stdout)
        self.logger = logging.getLogger(connector)

        sh = logging.handlers.SysLogHandler('/dev/log', logging.handlers.SysLogHandler.LOG_USER)
        sh.setFormatter(lf)
        sh.setLevel(lv)
        self.logger.addHandler(sh)

    def warn(self, msg):
        self.logger.warn(msg)

    def error(self, msg):
        self.logger.error(msg)

    def critical(self, msg):
        self.logger.critical(msg)

    def info(self, msg):
        self.logger.info(msg)

class AvroWriter:
    """ AvroWriter """
    def __init__(self, schema, outfile, listdata, name, logger):
        self.schema = schema
        self.listdata = listdata
        self.outfile = outfile
        self.logger = logger

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
