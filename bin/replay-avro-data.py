#!/usr/bin/python
import argparse
from argo_egi_connectors.output import AmsPublish, load_schema
from avro.datafile import DataFileReader
from avro.io import DatumReader


class AvroReader:
    def __init__(self, inputfile):
        self.inputfile = inputfile
        self.avrofile = None
        self.reader = None
        self._load_datareader()

    def _load_datareader(self):
        try:
            self.avrofile = open(self.inputfile, "rb")
            self.reader = DataFileReader(self.avrofile, DatumReader())
        except Exception:
            return False

    def read(self):
        try:
            if not self.reader or not self.avrofile:
                raise ("AvroFileReader not initialized.")

            data = []
            for d in self.reader:
                data.append(d)

            self.reader.close()
            self.avrofile.close()

        except Exception as e:
            return False, e

        return True, data


def main():
    parser = argparse.ArgumentParser(
        description="Helper tool that can replay avro data on AMS with "
                    "customizable datestamp"
    )
    parser.add_argument("-h", dest='host', nargs=1, metavar="ams_host",
                        help="ams host", type=str, required=True)
    parser.add_argument("-p", dest="project", nargs=1, metavar="ams_host",
                        help="ams project", type=str, required=True)
    parser.add_argument("-t", dest="token", nargs=1, metavar="ams_token",
                        help="ams token", type=str, required=True)
    parser.add_argument("-o", dest="topic", nargs=1, metavar="ams_topic",
                        help="ams topic", type=str, required=True)
    parser.add_argument("-r", dest="report", nargs=1, metavar="report",
                        help="report", type=str, required=True)
    parser.add_argument("-f", dest="avrofile", nargs=1, metavar="avro_file",
                        help="input avro file", type=str, required=True)
    parser.add_argument("-s", dest="schema", nargs=1, metavar="avro_schema",
                        help="avro schema", type=str, required=True)
    parser.add_argument("-m", dest="msgtype", metavar="msgtype",
                        help="message type", type=str, required=True)
    parser.add_argument("-d", dest="date", metavar="YEAR-MONTH-DAY",
                        help="custom date", type=str, required=True)
    args = parser.parse_args()

    avro = AvroReader(args.avrofile)
    ret, data = avro.read()
    if not ret:
        print "Error: " + data
        raise SystemExit(1)

    amshost = args.host  # ams host
    amsproject = args.project  # ams project
    amstoken = args.token  # ams token
    amsreport = args.report  # report - dirname from customer.conf is used for this one
    amsbulk = 100
    amspacksinglemsg = True
    connectionretry = 3

    ams = AmsPublish(amshost, amsproject, amstoken, amsreport, amsbulk,
                     amspacksinglemsg, connectionretry)

    ams.send(args.schema, args.msgtype, args.date, data)


if __name__ == "__main__":
    main()
