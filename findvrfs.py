import json
import os
import socket
import sqlite3
import sys
from argparse import ArgumentParser, FileType
from collections import defaultdict
from multiprocessing.pool import Pool
from typing import List, Set, Tuple, Dict

from bgp.routing_table import RoutingTable
from traceroute.warts import Warts
from traceroute.warts_trace import WartsTrace
from utils.progress import Progress
from utils.utils import File2

ip2as: RoutingTable = None


def family(a):
    if '.' in a:
        return socket.AF_INET
    else:
        return socket.AF_INET6


def pton(addr, fam=None):
    if fam is None:
        fam = family(addr)
    return int.from_bytes(socket.inet_pton(fam, addr), 'big')


def ntop(num: int, fam):
    length = 4 if fam == socket.AF_INET else 16
    return socket.inet_ntop(fam, num.to_bytes(length, 'big'))


def subnet31(x, y):
    x = pton(x)
    y = pton(y)
    if x < y:
        x, y = y, x
    if x % 2 == 1:
        return True
    return False


def otherside31(x):
    fam = family(x)
    x = pton(x, fam=fam)
    if x % 2 == 1:
        oside = x - 1
    else:
        oside = x + 1
    return ntop(oside, fam)


def extract_pairs(filename):
    pairs = set()
    basns = defaultdict(set)
    aasns = defaultdict(set)
    addrs = set()
    close = False
    if isinstance(filename, str):
        f = Warts(filename, json=True)
        close = True
        pb = Progress(message='Reading', increment=100000, callback=lambda: 'Pairs {:,d}'.format(len(pairs)))
    else:
        pb = Progress(len(filename), message='Reading', increment=10000, callback=lambda: 'Pairs {:,d}'.format(len(pairs)))
        f = filename
    for j in pb.iterator(f):
        if isinstance(j, dict):
            trace = WartsTrace(j, ip2as=ip2as)
        else:
            trace = j
        addrs.update(trace.addrs)
        for i in range(len(trace.hops) - 1):
            x = trace.hops[i]
            y = trace.hops[i+1]
            w = trace.hops[i-1] if i > 0 else None
            z = trace.hops[i+2] if i < len(trace.hops) - 2 else None
            # if y.icmp_type != 0:
            # if x.addr == '206.72.210.14' and y.addr == '206.72.210.63':
            #     print(x.asn, y.asn, -100 >= x.asn == y.asn, file=sys.stderr)
            xnum = pton(x.addr)
            ynum = pton(y.addr)
            if abs(xnum - ynum) == 1 or (-100 >= x.asn == y.asn):
                pairs.add((x.addr, y.addr))
                if w and w.addr != x.addr:
                    if w.asn != 0:
                        basns[x.addr, y.addr].add(w.asn)
                if z and z.addr != y.addr:
                    z = trace.hops[i+2]
                    if z.asn != 0:
                        aasns[x.addr, y.addr].add(z.asn)
    if close:
        f.close()
    return pairs, basns, aasns, addrs


def addasns(basns, aasns, aspaths):
    osums = (sum(len(v) for v in basns), sum(len(v) for v in aasns))
    sums = None
    while sums != osums:
        print('Again!')
        for addr, paths in aspaths.items():
            for x, y in paths:
                if x in basns[addr]:
                    aasns[addr].add(y)
                if y in aasns[addr]:
                    basns[addr].add(x)
        osums, sums = sums, (sum(len(v) for v in basns), sum(len(v) for v in aasns))


def find_pairs(filenames: List[str], poolsize: int):
    pairs: Set[Tuple[str, str]] = set()
    basns = defaultdict(set)
    aasns = defaultdict(set)
    addrs: Set[str] = set()
    oldoutput = Progress.should_output
    try:
        Progress.set_output(True)
        pb = Progress(len(filenames), 'Processing traceroutes', callback=lambda: ' Pairs {:,d} Addrs {:,d}'.format(len(pairs), len(addrs)))
        Progress.set_output(False)
        with Pool(poolsize) as pool:
            for newpairs, newbasns, newaasns, newaddrs in pb.iterator(pool.imap_unordered(extract_pairs, filenames)):
                addrs.update(newaddrs)
                pairs.update(newpairs)
                for k, v in newbasns.items():
                    basns[k].update(v)
                for k, v in newaasns.items():
                    aasns[k].update(v)
    finally:
        Progress.set_output(oldoutput)
    return pairs, basns, aasns, addrs


def remove_internal(pairs: Set[Tuple[str, str]], basns: Dict[Tuple[str, str], Set[int]], aasns: Dict[Tuple[str, str], Set[int]], addrs: Set[str]):
    pairs2 = set()
    ba = {}
    aa = {}
    pb = Progress(len(pairs), 'Checking', increment=100000, callback=lambda: 'Pairs {:,d} BASNs {:,d} AASNs {:,d}'.format(len(pairs2), len(ba), len(aa)))
    for x, y in pb.iterator(pairs):
        osidex = otherside31(x)
        osidey = otherside31(y)
        # if ip2as[x] < 0:
        if ip2as[x] < 0 or osidex == y or (osidex not in addrs and osidey not in addrs):
            if ip2as[x] not in basns[x, y] & aasns[x, y]:
                pairs2.add((x, y))
                ba[x, y] = basns[x, y]
                aa[x, y] = aasns[x, y]
    return pairs2, ba, aa


def save_sql(filename, pairs, basns, aasns, replace=True):
    if replace:
        if os.path.exists(filename):
            os.remove(filename)
    con = sqlite3.connect(filename)
    con.execute('''CREATE TABLE pairs (
    x TEXT,
    y TEXT,
    before TEXT,
    after TEXT
    )''')
    values = []
    for x, y in pairs:
        before = json.dumps(list(basns[x, y]))
        after = json.dumps(list(aasns[x, y]))
        values.append([x, y, before, after])
    con.executemany('INSERT INTO pairs (x, y, before, after) VALUES (?, ?, ?, ?)', values)
    con.commit()
    con.close()


def main():
    global ip2as
    parser = ArgumentParser()
    parser.add_argument('-i', '--ip2as', help='IP2AS mappings')
    parser.add_argument('-W', '--warts', type=FileType('r'), help='File containing warts filenames.')
    parser.add_argument('-A', '--atlas', type=FileType('r'), help='File containing atlas filenames.')
    parser.add_argument('-w', '--single-warts', help='Single warts file.')
    parser.add_argument('-p', '--poolsize', default=1, type=int, help='Number of parallel processes.')
    args = parser.parse_args()
    ip2as = RoutingTable.ip2as(args.ip2as)
    with File2(args.files) as f:
        files = [line.strip() for line in f if not line.startswith('#')]
    pairs, basns, aasns, addrs = find_pairs(files, poolsize=args.poolsize)
    pairs, basns, aasns = remove_internal(pairs, basns, aasns, addrs)
    
