import csv
from argparse import ArgumentParser
from collections import Counter, namedtuple
from itertools import combinations
import os
from sys import stderr
from traceback import TracebackException

import numpy as np
import pandas as pd
from logging import getLogger, INFO

from multiprocessing import Queue, Process, Pipe, current_process

import sys

import bgp.routing_table as rt
from as2org import AS2Org
from bgp.bgp import BGP
from traceroute.warts import Warts
from utils.progress import Progress
from utils.utils import ls

log = getLogger()
log.setLevel(INFO)

ip2as = None

Outputs = namedtuple('Outputs', ['Addrs', 'Adjs', 'DPs', 'Dists'])
Adjacency = namedtuple('Adjacency', ['Hop1', 'Hop2', 'Distance', 'Private', 'Suspicious', 'Type'])
DestPair = namedtuple('DestPair', ['Interface', 'DestASN', 'Suspicious', 'Loop'])
Distance = namedtuple('Distance', ['Hop1', 'Hop2', 'Distance'])
addresses_output = True
adjacencies_output = True
destpairs_output = True
distances_output = True
output_dir = None
TB = namedtuple('TB', ['value'])
keep_files = True
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


def process_trace_file(jobs, outputs, status):
    try:
        p = current_process()
        pid = p.pid
        name = p.name
        while True:
            addresses = set()
            adjacencies = set()
            destpairs = set()
            distances = Counter()
            filename = jobs.get()
            if filename is None:
                break
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
                                    destpairs.add((hop2['addr'], dest_asn, False, loop))
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
                                        destpairs.add((addr2, dest_asn, issuspicious, loop))
                                    if addr1 != addr2:  # Skip links with the same address
                                        if qttl2 == 0:
                                            if not (hop3 and (addr2 == hop3['addr'] or hop2['reply_ttl'] - hop3['reply_ttl'] == (hop3['probe_ttl'] - hop2['probe_ttl']) - 1)):
                                                qttl2 = 1
                                        elif qttl2 > 1:
                                            if not (hop2['icmp_type'] == 3 and hop2['icmp_q_ttl'] - hop1['icmp_q_ttl'] >= hop2['probe_ttl'] - hop1['probe_ttl']):
                                                qttl1 = 1
                                                qttl2 = 1
                                        if qttl1 == 0:
                                            distance -= 1
                                        if qttl2 == 0:
                                            distance += 1
                                        if distance == 1:
                                            link_type = 1
                                        elif distance <= 0:
                                            link_type = -1
                                        else:
                                            link_type = 2
                                        adjacencies.add((addr1, addr2, link_type, hop1['private'], issuspicious, hop1['icmp_type']))
                                # for hop1, hop2 in combinations(hopslist, 2):
                                #     addr1 = hop1['addr']
                                #     addr2 = hop2['addr']
                                #     probe_ttl1 = hop1['probe_ttl']
                                #     probe_ttl2 = hop2['probe_ttl']
                                #     if addr1 != addr2 and probe_ttl1 != probe_ttl2:
                                #         distances[(addr1, addr2)] += 1 if probe_ttl2 - probe_ttl1 == 1 else -1
            # output.put((addresses, adjacencies, destpairs, distances))
            ofilename = os.path.splitext(os.path.basename(filename))[0]
            if destpairs_output and destpairs:
                ofile = os.path.join(output_dir, 'dps', ofilename + '.csv')
                write_destpairs(ofile, destpairs)
                outputs.DPs.put(ofile)
            if distances_output and distances:
                ofile = os.path.join(output_dir, 'dists', ofilename + '.csv')
                write_distances(ofile, distances)
                outputs.Dists.put(ofile)
            if adjacencies_output and adjacencies:
                ofile = os.path.join(output_dir, 'adjs', ofilename + '.csv')
                write_adjacencies(ofile, adjacencies)
                outputs.Adjs.put(ofile)
            if addresses_output and addresses:
                ofile = os.path.join(output_dir, 'addrs', ofilename + '.txt')
                write_addresses(ofile, addresses)
                outputs.Addrs.put(ofile)
            status.put((pid, 'Jobs', 1))
        for q in outputs:
            q.put(None)
        status.put((pid, None, 1))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = TracebackException(exc_type, exc_value, exc_traceback)
        t = TB(''.join(tb.format()))
        status.put(t)


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
            with open(filename) as f:
                f.readline()
                for line in f:
                    h1, h2, n = line.split(',')
                    s[(h1, h2)] += int(n)
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


def run(files, poolsize):
    jobs = Queue()  # Traceroute filenames will be put here for the works
    outputs = Outputs(*[Queue() for _ in Outputs._fields])  # Workers put completed filenames here for aggs to read
    status = Queue()
    # Create the workers
    workers = {}
    for i in range(poolsize):
        p = Process(target=process_trace_file, args=(jobs, outputs, status), name='Jobs')
        p.start()
        workers[p.pid] = p
    aggs = [
        Process(target=combine, args=(outputs.Addrs, os.path.join(output_dir, 'addrs.txt'), status, poolsize), name='Addrs'),
        Process(target=combine, args=(outputs.Adjs, os.path.join(output_dir, 'adjs.csv'), status, poolsize), name='Adjs'),
        Process(target=combine, args=(outputs.DPs, os.path.join(output_dir, 'dps.csv'), status, poolsize), name='DPs'),
        Process(target=combine_dists, args=(outputs.Dists, os.path.join(output_dir, 'dists.csv'), status, poolsize), name='Dists')
    ]
    # Create the aggs
    for p in aggs:
        p.start()
        workers[p.pid] = p
    try:
        # Put filenames into the jobs queue
        for filename in files:
            jobs.put(filename)
        # Put the None markers into the jobs queue, one for each worker
        for _ in range(poolsize):
            jobs.put(None)
        # Track the status
        cj = {'Jobs': 0, 'Addrs': 0, 'Adjs': 0, 'DPs': 0, 'Dists': 0}
        sizes = {'Addrs': 0, 'Adjs': 0, 'DPs': 0, 'Dists': 0}
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
    finally:
        for k, p in workers.items():
            print(k)
            p.terminate()
            p.join()


def main():
    # global ip2as, output_dir, keep_files, addresses_output, adjacencies_output, destpairs_output, distances_output
    global ip2as, args
    parser = ArgumentParser()
    parser.add_argument('-a', '--adj', action='store_true', help='Adjacency output file.')
    parser.add_argument('-b', '--addr', action='store_true', help='Addresses output file.')
    parser.add_argument('-d', '--dp', action='store_true', help='Dest pairs output file.')
    parser.add_argument('-e', '--dist', action='store_true', help='Distances between interfaces.')
    # parser.add_argument('-g', '--day', dest='day', type=int, help='Day.')
    parser.add_argument('-f', '--files', help='Unix-style file regex.')
    # parser.add_argument('-m', '--month', dest='month', type=int, help='Month.')
    # parser.add_argument('-r', '--rir', action='store_true', help='Use RIR delegations to fill in missing prefixes.')
    # parser.add_argument('-y', '--year', dest='year', type=int, help='Year.')
    parser.add_argument('-k', '--keep', action='store_true', help='Keep the intermediate files.')
    parser.add_argument('-o', '--output-dir', default='.', help='Directory where the output files will be written.')
    parser.add_argument('-p', '--poolsize', default=1, type=int, help='Number of parallel processes.')
    parser.add_argument('--prefixes', help='BGP prefix file to use.')
    args = parser.parse_args()
    print(args)
    # if not args.files:
    #     fregex = 'caida/traces/team-*/daily/*/cycle-*/*{{{:06d}..{:06d}}}.*'.format(args.first_cycle, args.last_cycle)
    # else:
    #     fregex = args.files
    # files = list(ls(fregex))
    # log.info('Number of files {:,d}'.format(len(files)))
    # kargs = dict(year=args.year, month=args.month, day=args.day, prefixes=args.prefixes, rir=args.rir)
    # as2org = AS2Org(include_potaroo=False, **kargs)
    # bgp = BGP(**kargs)
    # ip2as = rt.default_routing_table(as2org=as2org, bgp=bgp, **kargs)
    # print('Cleaning up...', file=stderr)


if __name__ == '__main__':
    main()