#!/usr/bin/python

from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import argparse, os, pprint, errno
import avro.schema

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', nargs=1, help='new schema', required=True, metavar='avro schema')
    parser.add_argument('-i', nargs='+', help='avro files', required=True, metavar='avro file')
    parser.add_argument('-ts', action='store_true', help='convert int tag values to str', required=False)
    parser.add_argument('-o', nargs=1, help='output directory', required=True, metavar='output directory')
    args = parser.parse_args()

    for f in args.i:
        out = []

        if args.o[0].startswith('/'):
            dest = args.o[0]
        else:
            dest = os.path.abspath('.') + '/' + args.o[0]

        try:
            os.makedirs(dest)
        except OSError as e:
            if e.args[0] != errno.EEXIST:
                print os.strerror(e.args[0]), e.args[1], args.o[0]
                raise SystemExit(1)

        schema = avro.schema.parse(open(args.s[0]).read())
        writer = DataFileWriter(open(dest + '/' + os.path.basename(f), 'w'), DatumWriter(), schema)
        reader = DataFileReader(open(f, 'r'), DatumReader())

        try:
            for i, entry in enumerate(reader):
                if args.ts:
                    for t in entry['tags']:
                        if isinstance(entry['tags'][t], int):
                            entry['tags'][t] = str(entry['tags'][t])
                writer.append(entry)

            writer.close()

        except UnicodeDecodeError as e:
            pprint.pprint(e)
            print f

main()
