#!/usr/bin/env python
import os
import re
from argparse import ArgumentParser

from sqlalchemy import create_engine, MetaData, Table, Column, Text, Integer
from sqlalchemy.engine import ResultProxy

from bgp.routing_table import RoutingTable
from utils.progress import Progress
from utils.utils import File2


def opendb(filename, remove=False):
    if remove:
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
    engine = create_engine('sqlite:///{}'.format(filename))
    meta = MetaData()
    annotation = Table(
        'annotation', meta,
        Column('addr', Text),
        Column('asn', Integer),
        Column('conn_asn', Integer)
    )
    meta.create_all(engine)
    return engine, meta


def parse(filename, output, ip2as, monitor=None, conn_asn=None, db='monitors.db', chunksize=100000):
    if conn_asn is None:
        if monitor is None:
            basename = os.path.basename(filename)
            monitor = re.match(r'([-a-z0-9]+)\.', basename).group(1)
            print(monitor)
        engine = create_engine('sqlite:///{}'.format(db))
        result: ResultProxy = engine.execute('SELECT asn FROM monitor WHERE monitor = ?', monitor)
        conn_asn, = result.fetchone()
    engine, meta = opendb(output, remove=True)
    annotations = 0
    values = []
    pb = Progress(message='Parsing bdrmap output', increment=10000, callback=lambda: 'Annot {:,d}'.format(annotations))
    internal = False
    with File2(filename) as f:
        for line in pb.iterator(f):
            if line:
                m = re.match(r'owner (\d+)', line)
                if m:
                    vpasn = int(m.group(1))
                    if vpasn > 0:
                        internal = True
                    else:
                        internal = False
                    continue
                if line[0] == ' ':
                    asn = int(re.match(r'\s+(\d+)\s', line).group(1))
                    addrs = re.findall(r'\d+\.\d+\.\d+\.\d+\*?', line)
                    for addr in addrs:
                        local_conn_asn = conn_asn
                        if addr[-1] == '*':
                            addr = addr[:-1]
                        else:
                            local_conn_asn = ip2as[addr]
                        if ip2as[addr] == -1:
                            local_conn_asn = -1
                        values.append(dict(addr=addr, asn=asn, conn_asn=local_conn_asn))
                        annotations += 1
                elif internal:
                    addr = re.search(r'(\d+\.\d+\.\d+\.\d+)\*', line)
                    if addr:
                        addr = addr.group(1)
                        values.append(dict(addr=addr, asn=conn_asn, conn_asn=conn_asn))
                        annotations += 1
                if len(values) == chunksize:
                    engine.execute(meta.tables['annotation'].insert(), values)
                    values = []
    if values:
        engine.execute(meta.tables['annotation'].insert(), values)


def main():
    parser = ArgumentParser()
    parser.add_argument('-f', '--filename', required=True, help='bdrmap output file.')
    parser.add_argument('-o', '--output', required=True, help='Output sqlite DB filename.')
    parser.add_argument('-i', '--ip2as', required=True)
    parser.add_argument('-m', '--monitor')
    parser.add_argument('-a', '--asn', type=int)
    args = parser.parse_args()
    ip2as = RoutingTable.ip2as(args.ip2as)
    parse(args.filename, args.output, ip2as, monitor=args.monitor, conn_asn=args.asn)


if __name__ == '__main__':
    main()
