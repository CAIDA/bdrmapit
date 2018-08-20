#!/usr/bin/env python
import sqlite3
from argparse import ArgumentParser
from multiprocessing.pool import Pool
from os import makedirs
import os.path

from as2org import AS2Org
from bgp.routing_table import RoutingTable
from traceroute.abstract_trace import AbstractTrace
from traceroute.output_type import OutputType
from traceroute.parser import Parser, opendb
from utils.progress import Progress
from utils.utils import File2


table = '''CREATE TABLE annotation (
  addr TEXT,
  asn INT,
  org TEXT,
  conn_asn INT,
  conn_org TEXT,
  distance INT
)'''


ip2as: RoutingTable = None
as2org: AS2Org = None
output_dir: str = None


class NaiveParse:

    def __init__(self, filename, output_type, ip2as, as2org):
        self.filename = filename
        self.output_type = output_type
        self.ip2as = ip2as
        self.as2org = as2org
        self.values = []
        self.parser = Parser(filename, output_type, ip2as)

    def parse_separate(self):
        pb = Progress(message='Parsing the traceroutes', increment=1000)
        Progress.set_output(False)
        for trace in pb.iterator(self.parser):
            self.singlerun(trace)

    def singlerun(self, trace: AbstractTrace):
        for x, y in zip(trace.hops, trace.hops[1:]):
            d = (y.addr, y.asn, self.as2org[y.asn], x.asn, self.as2org[x.asn], y.ttl - x.ttl)
            self.values.append(d)

    def save(self, filename, remove=False, chunksize=10000):
        if remove:
            try:
                os.remove(filename)
            except FileNotFoundError:
                pass
        con: sqlite3.Connection = sqlite3.connect(filename)
        con.execute(table)
        con.commit()
        query = 'INSERT INTO annotation (addr, asn, org, conn_asn, conn_org, distance) VALUES (?, ?, ?, ?, ?, ?)'
        for i in range(0, len(self.values) + 1, chunksize):
            cur = con.cursor()
            cur.executemany(query, self.values[i:i + chunksize])
            cur.close()
            con.commit()
        con.close()


def run(filename):
    basename = os.path.basename(filename).rpartition('.')[0]
    output = '{}.db'.format(basename)
    output = os.path.join(output_dir, output)
    fp = NaiveParse(filename, OutputType.warts, ip2as, as2org)
    fp.parse_separate()
    fp.save(output, remove=True)


def main():
    global ip2as, as2org, bgp, output_dir
    parser = ArgumentParser()
    parser.add_argument('-i', '--ip2as', required=True, help='BGP prefix file regex to use.')
    parser.add_argument('-a', '--as2org', required=True, help='AS-to-Org mappings in the standard CAIDA format.')
    parser.add_argument('-p', '--parallel', type=int)
    parser.add_argument('-o', '--output', required=True, help='Results database file.')
    parser.add_argument('-R', '--remove', action='store_true', help='Remove file if it exists')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--filename', help='Traceroute filename')
    group.add_argument('-F', '--file-list')
    args = parser.parse_args()
    as2org = AS2Org(args.as2org, include_potaroo=False)
    ip2as = RoutingTable.ip2as(args.ip2as)
    output_dir = args.output
    makedirs(output_dir, exist_ok=True)
    if args.filename:
        filenames = [args.filename]
    else:
        with File2(args.file_list) as f:
            filenames = [l.strip() for l in f]
    if not args.parallel or len(filenames) == 1:
        for filename in filenames:
            run(filename)
    else:
        pb = Progress(len(filenames), 'Running bdrmapIT single traceroute')
        Progress.set_output(False)
        with Pool(args.parallel) as pool:
            for _ in pb.iterator(pool.imap_unordered(run, filenames)):
                pass


if __name__ == '__main__':
    main()
