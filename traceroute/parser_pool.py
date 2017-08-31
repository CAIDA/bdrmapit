import csv
import os
import sys
from argparse import ArgumentParser
from collections import Counter, namedtuple
from logging import getLogger, INFO
from multiprocessing import Queue, Process, current_process
from sys import stderr
from traceback import TracebackException

import numpy as np
import pandas as pd

import bgp.routing_table as rt
from traceroute.warts import Warts
from utils.utils import ls, File2

log = getLogger()
log.setLevel(INFO)

ip2as = None

Outputs = namedtuple('Outputs', ['Addrs', 'Adjs', 'DPs', 'Dists'])
Adjacency = namedtuple('Adjacency', ['Hop1', 'Hop2', 'Distance', 'Private', 'Suspicious', 'Type'])
DestPair = namedtuple('DestPair', ['Interface', 'DestASN', 'Echo', 'Suspicious', 'Loop'])
Distance = namedtuple('Distance', ['Hop1', 'Hop2', 'Distance'])
TB = namedtuple('TB', ['value'])
args = None


def extract_trace(hops, hop_count):
    trace = np.full(hop_count, fill_value=np.nan, dtype='object')
    hopslist = []
    for hop in hops:
        address = hop['addr']
        ttl = hop['probe_ttl'] - 1
        if pd.isnull(trace[ttl]):
            asn = ip2as[address]
            hop['asn'] = asn
            trace[ttl] = address
            hopslist.append(hop)
        elif trace[ttl] and trace[ttl] != address:
            trace[ttl] = False
            hopslist.pop()
    return hopslist


def remove_loops(hopslist):
    seen = set()
    previous = None
    end = 0
    for i, hop in enumerate(reversed(hopslist), 1):
        addr = hop['addr']
        if addr != previous:
            if addr in seen:
                end = i-1
            previous = addr
            seen.add(addr)
    return hopslist[:len(hopslist)-end]


def remove_private(hopslist):
    newlist = []
    for hop in hopslist:
        hop['private'] = False
        if hop['asn'] > -2:
            newlist.append(hop)
        elif newlist:
            newlist[-1]['private'] = True
    return newlist


def suspicious_hop(hopslist, dest_asn):
    for hop in hopslist[::-1]:
        asn = hop['asn']
        if asn == dest_asn:
            return hop['probe_ttl']
    return hopslist[-1]['probe_ttl']


class Set2(set):

    def __init__(self, use, *a, **k):
        super().__init__(*a, **k)
        if not use:
            self.add = lambda x: None
            self.update = lambda x: None


class Counter2(Counter):

    @classmethod
    def fromkeys(cls, iterable, v=None):
        raise NotImplementedError()

    def __init__(self, use, *a, **k):
        super().__init__(*a, **k)
        self.use = use
        if not use:
            # self.__setitem__ = lambda x, y: None
            self.update = lambda x: None

    def __setitem__(self, key, value):
        if self.use:
            super().__setitem__(key, value)


def worker_func(jobs, outputs, status):
    try:
        p = current_process()
        pid = p.pid
        name = p.name
        while True:
            filename = jobs.get()
            if filename is None:
                break
            process_trace_file(filename, outputs)
            status.put((pid, name, 1))
        for q in outputs:
            q.put(None)
        status.put((pid, None, 1))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = TracebackException(exc_type, exc_value, exc_traceback)
        t = TB(''.join(tb.format()))
        status.put(t)


def process_trace_file(filename, outputs=None):
    addresses = Set2(args.addr)
    adjacencies = Set2(args.adj)
    destpairs = Set2(args.dp)
    distances = Counter2(args.dist)
    with Warts(filename, json=True) as f:
        for j in f:
            if j['type'] == 'trace':
                original_hopslist = j.get('hops')
                if original_hopslist:
                    for hop in original_hopslist:
                        addresses.add(hop['addr'])
                    hopslist = extract_trace(original_hopslist, j['hop_count'])
                    hopslist = remove_private(hopslist)
                    hopslist = remove_loops(hopslist)
                    if hopslist:
                        numhops = len(hopslist)
                        dst = j['dst']
                        dest_asn = ip2as[dst]
                        suspicious = suspicious_hop(hopslist, dest_asn)
                        loop = j['stop_reason'] == 'LOOP'
                        i = 0
                        hop2 = hopslist[0]
                        qttl2 = hop2.get('icmp_q_ttl', 1)
                        addr2 = hop2['addr']
                        if hop2['asn'] > -2:
                            destpairs.add((addr2, dest_asn, hop2['icmp_type'] == 0, False, loop))
                        while i < numhops - 1:
                            i += 1
                            hop1, hop2 = hop2, hopslist[i]
                            hop3 = hopslist[i + 1] if i <= numhops - 2 else None
                            qttl1, qttl2 = qttl2, hop2.get('icmp_q_ttl', 1)
                            addr1, addr2 = addr2, hop2['addr']
                            distance = hop2['probe_ttl'] - hop1['probe_ttl']
                            distances[(addr1, addr2)] += 1 if distance == 1 else -1
                            if hop3:
                                distances[(addr1, hop3['addr'])] -= 1
                            issuspicious = hop2['probe_ttl'] > suspicious
                            if hop2['asn'] > -2:
                                destpairs.add((addr2, dest_asn, hop2['icmp_type'] == 0, issuspicious, loop))
                            if addr1 != addr2:  # Skip links with the same address
                                if qttl2 == 0:
                                    if not (hop3 and (
                                            addr2 == hop3['addr'] or hop2['reply_ttl'] - hop3['reply_ttl'] == (
                                        hop3['probe_ttl'] - hop2['probe_ttl']) - 1)):
                                        qttl2 = 1
                                elif qttl2 > 1:
                                    if not (hop2['icmp_type'] == 3 and hop2['icmp_q_ttl'] - hop1['icmp_q_ttl'] >= hop2[
                                        'probe_ttl'] - hop1['probe_ttl']):
                                        qttl1 = 1
                                        qttl2 = 1
                                distance -= qttl2 - qttl1
                                if distance == 1:
                                    link_type = 1
                                elif distance <= 0:
                                    link_type = -1
                                else:
                                    link_type = 2
                                adjacencies.add(
                                    (addr1, addr2, link_type, hop1['private'], issuspicious, hop2['icmp_type']))
    ofilename = os.path.splitext(os.path.basename(filename))[0]
    if args.dp:
        ofile = os.path.join(args.output_dir, 'dps', ofilename + '.csv')
        write_destpairs(ofile, destpairs)
        if outputs is not None:
            outputs.DPs.put(ofile)
    if args.dist:
        ofile = os.path.join(args.output_dir, 'dists', ofilename + '.csv')
        write_distances(ofile, distances)
        if outputs is not None:
            outputs.Dists.put(ofile)
    if args.adj:
        ofile = os.path.join(args.output_dir, 'adjs', ofilename + '.csv')
        write_adjacencies(ofile, adjacencies)
        if outputs is not None:
            outputs.Adjs.put(ofile)
    if args.addr:
        ofile = os.path.join(args.output_dir, 'addrs', ofilename + '.txt')
        write_addresses(ofile, addresses)
        if outputs is not None:
            outputs.Addrs.put(ofile)


def combine(fqueue, ofile, status, poolsize):
    try:
        header = ''
        p = current_process()
        pid = p.pid
        name = p.name
        s = set()
        while True:
            filename = fqueue.get()
            if filename is None:
                poolsize -= 1
                if poolsize == 0:
                    break
                continue
            with open(filename) as f:
                header = f.readline()
                s.update(f)
            if not args.keep:
                os.remove(filename)
            status.put((pid, name, len(s)))
        with open(ofile, 'w') as f:
            f.write(header)
            f.writelines(s)
        status.put((pid, None, len(s)))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = TracebackException(exc_type, exc_value, exc_traceback)
        t = TB(''.join(tb.format()))
        status.put(t)


def combine_dists(fqueue, ofile, status, poolsize):
    try:
        p = current_process()
        pid = p.pid
        name = p.name
        s = Counter()
        while True:
            filename = fqueue.get()
            if filename is None:
                poolsize -= 1
                if poolsize == 0:
                    break
                continue
            for hop1, hop2, n in pd.read_csv(filename).itertuples(index=False, name=None):
                s[(hop1, hop2)] += n
            if not args.keep:
                os.remove(filename)
            status.put((pid, name, len(s)))
        write_distances(ofile, s, nonegative=True)
        status.put((pid, None, len(s)))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = TracebackException(exc_type, exc_value, exc_traceback)
        t = TB(''.join(tb.format()))
        status.put(t)


def write_addresses(filename, addresses):
    with open(filename, 'w') as f:
        f.write('Addresses\n')
        f.writelines('{}\n'.format(address) for address in addresses)


def write_adjacencies(filename, adjacencies):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(Adjacency._fields)
        writer.writerows(adjacencies)


def write_destpairs(filename, destpairs):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(DestPair._fields)
        writer.writerows(destpairs)


def write_distances(filename, distances, nonegative=False):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(Distance._fields)
        if nonegative:
            writer.writerows([[hop1, hop2, num] for (hop1, hop2), num in distances.items() if num > 0])
        else:
            writer.writerows([[hop1, hop2, num] for (hop1, hop2), num in distances.items()])


def create_combiners(func, queue, filename, status, poolsize, name, workers, cj, sizes):
    p = Process(target=func, args=(queue, filename, status, poolsize), name=name)
    p.start()
    workers[p.pid] = p
    cj[name] = 0
    sizes[name] = 0


def mkdirs():
    os.makedirs(os.path.join(args.output_dir, 'addrs'), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'adjs'), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'dps'), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'dists'), exist_ok=True)


def run(files, poolsize):
    if len(files) > 1:
        mkdirs()
        jobs = Queue()  # Traceroute filenames will be put here for the works
        outputs = Outputs(*[Queue() for _ in Outputs._fields])  # Workers put completed filenames here for aggs to read
        status = Queue()
        workers = {}
        cj = {'Jobs': 0}
        sizes = {}
        for i in range(poolsize):
            p = Process(target=worker_func, args=(jobs, outputs, status), name='Jobs')
            p.start()
            workers[p.pid] = p
        if args.addr:
            create_combiners(combine, outputs.Addrs, os.path.join(args.output_dir, 'addrs.txt'), status, poolsize, 'Addrs', workers, cj, sizes)
        if args.adj:
            create_combiners(combine, outputs.Adjs, os.path.join(args.output_dir, 'adjs.csv'), status, poolsize, 'Adjs', workers, cj, sizes)
        if args.dp:
            create_combiners(combine, outputs.DPs, os.path.join(args.output_dir, 'dps.csv'), status, poolsize, 'DPs', workers, cj, sizes)
        if args.dist:
            create_combiners(combine_dists, outputs.Dists, os.path.join(args.output_dir, 'dists.csv'), status, poolsize, 'Dists', workers, cj, sizes)
        try:
            # Put filenames into the jobs queue
            for filename in files:
                jobs.put(filename)
            # Put the None markers into the jobs queue, one for each worker
            for _ in range(poolsize):
                jobs.put(None)
            # Track the status
            while workers:
                a = status.get()
                if isinstance(a, Exception):
                    raise a
                if isinstance(a, TB):
                    print(a.value)
                    raise Exception('Custom')
                pid, job, num = a
                if job is None:
                    p = workers.pop(pid)
                    p.join()
                else:
                    cj[job] += 1
                    if job != 'Jobs':
                        sizes[job] = num
                s = []
                for i, n in cj.items():
                    s.append('{} {:,d} ({:.2%}) {:,d}'.format(i, n, n / len(files), sizes.get(i, 0)))
                sys.stderr.write('\r\033[K{}'.format(' '.join(s)))
            sys.stderr.write('\n')
        finally:
            for k, p in workers.items():
                print(k)
                p.terminate()
                p.join()
    else:
        mkdirs()
        for filename in files:
            process_trace_file(filename)


def main():
    global ip2as, args
    parser = ArgumentParser()
    parser.add_argument('-a', '--adj', action='store_true', help='Adjacency output file.')
    parser.add_argument('-b', '--addr', action='store_true', help='Addresses output file.')
    parser.add_argument('-d', '--dp', action='store_true', help='Dest pairs output file.')
    parser.add_argument('-e', '--dist', action='store_true', help='Distances between interfaces.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--files', help='Unix-style file regex.')
    group.add_argument('-F', '--file-list', help='File containing traceroute filenames, one per line.')
    parser.add_argument('-k', '--keep', action='store_true', help='Keep the intermediate files.')
    parser.add_argument('-o', '--output-dir', default='.', help='Directory where the output files will be written.')
    parser.add_argument('-p', '--poolsize', default=1, type=int, help='Number of parallel processes.')
    parser.add_argument('-i', '--ip2as', required=True, help='BGP prefix file regex to use.')
    args = parser.parse_args()
    if args.files:
        files = list(ls(args.files))
    else:
        with File2(args.file_list) as f:
            files = list(map(str.strip, f))
    log.info('Number of files {:,d}'.format(len(files)))
    ip2as = rt.RoutingTable.ip2as(args.ip2as)
    run(files, args.poolsize)
    print('Cleaning up...', file=stderr)
