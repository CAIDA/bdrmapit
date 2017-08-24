import csv
import multiprocessing
from argparse import ArgumentParser
from collections import namedtuple, Counter
import os
from itertools import combinations
from sys import stderr, stdout

import numpy as np
import pandas as pd
import re

from logging import getLogger, INFO

from multiprocessing import Queue, Process

import bgp.routing_table as rt
from as2org import AS2Org
from bgp.bgp import BGP
from utils.progress import Progress
from traceroute.warts import Warts
from utils.utils import ls

log = getLogger()
log.setLevel(INFO)

DONE = 'DONE'

ip2as = None
addresses_output = None
adjacencies_output = None
dest_pairs_output = None
distances_output = None
addrqueue = None
adjqueue = None
dpqueue = None
distqueue = None
testqueue = None
error = False
record_vp = False
trace_regex = {
    '03-2016': 'caida/traces/team-*/daily/2016/cycle-*/*c0046{02..31}.*',
    '09-2016': 'caida/traces/team-*/daily/2016/cycle-*/*{5013..5050}.*',
    '02-2017': 'caida/traces/team-*/daily/2017/cycle-*/*{5397..5431}.*'
}
Adjacency = namedtuple('Adjacency', ['Hop1', 'Hop2', 'Distance', 'Private', 'Suspicious', 'Type'])
DestPair = namedtuple('DestPair', ['Interface', 'DestASN', 'Suspicious', 'Loop'])
Distance = namedtuple('Distance', ['Hop1', 'Hop2', 'Distance'])


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


def extract_vp(filename):
    m = re.search(r'daily\.l7\.t\d\.c(\d+)\.\d+\.([a-z0-9\-]+)\.warts\.gz', filename)
    cycle, vp = m.groups()
    return cycle, vp


def hop_distance(hop):
    ttl = hop['probe_ttl']
    qttl = hop.get('icmp_q_ttl', 1)
    return ttl + (1 - qttl)


def calculate_distance(hop1, hop2):
    ttl1 = hop1['probe_ttl']
    ttl2 = hop2['probe_ttl']
    qttl1 = hop1.get('icmp_q_ttl', 1)
    qttl2 = hop2.get('icmp_q_ttl', 1)
    if ttl2 - ttl1 == 1:
        if qttl1 == qttl2:
            return 1
        elif qttl2 == qttl1 + 1:
            return 1
        elif qttl2 > qttl1 + 1:
            return -1
    else:
        if qttl1 < qttl2:
            return 2
        else:
            return -1


def calculate_rttl_distance(hop1, hop2):
    return hop1['reply_ttl'] - hop2['reply_ttl']


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


def extract_distances(hopstlist):
    for hop1, hop2 in combinations(hopstlist, 2):
        addr1 = hop1['addr']
        addr2 = hop2['addr']
        probe_ttl1 = hop1['probe_ttl']
        probe_ttl2 = hop2['probe_ttl']
        if addr1 != addr2 and probe_ttl1 != probe_ttl2:
            distqueue.put((addr1, addr2), 1 if probe_ttl2 - probe_ttl1 == 1 else -1)


def process_trace_file(filename, parallel=False, unknown=None):
    # r = Counter()
    addresses = set()
    adjacencies = set()
    dest_pairs = set()
    distances = Counter()
    with Warts(filename, json=True) as f:
        for j in f:
            if j['type'] == 'trace':
                original_hopslist = j.get('hops')
                if original_hopslist:
                    if addrqueue:
                        for hop in original_hopslist:
                            addrqueue.put(hop['addr'])
                    # addresses.update(hop['addr'] for hop in original_hopslist)
                    # original_last_hop = original_hopslist[-1]
                    # original_last_distance = hop_distance(original_last_hop)
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
                        if dpqueue:
                            if hop2['asn'] > -2:
                                dest_pairs.add((hop2['addr'], dest_asn, False, loop))
                        while i < numhops - 1:
                            i += 1
                            hop1, hop2 = hop2, hopslist[i]
                            qttl1, qttl2 = qttl2, hop2.get('icmp_q_ttl', 1)
                            addr1, addr2 = addr2, hop2['addr']
                            issuspicious = hop2['probe_ttl'] > suspicious
                            if dpqueue:
                                if hop2['asn'] > -2:
                                    dpqueue.put((addr2, dest_asn, issuspicious, loop))
                            if adjqueue:
                                if addr1 != addr2:  # Skip links with the same address
                                    hop3 = hopslist[i+1] if i <= numhops - 2 else None
                                    if qttl2 == 0:
                                        if not (hop3 and (addr2 == hop3['addr'] or hop2['reply_ttl'] - hop3['reply_ttl'] == (hop3['probe_ttl'] - hop2['probe_ttl']) - 1)):
                                            qttl2 = 1
                                    elif qttl2 > 1:
                                        if not (hop2['icmp_type'] == 3 and hop2['icmp_q_ttl'] - hop1['icmp_q_ttl'] >= hop2['probe_ttl'] - hop1['probe_ttl']):
                                            qttl1 = 1
                                            qttl2 = 1
                                    distance = hop2['probe_ttl'] - hop1['probe_ttl']
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
                                    adjqueue.put((addr1, addr2, link_type, hop1['private'], issuspicious, hop1['icmp_type']))
                        if distqueue:
                            for hop1, hop2 in combinations(hopslist, 2):
                                addr1 = hop1['addr']
                                addr2 = hop2['addr']
                                probe_ttl1 = hop1['probe_ttl']
                                probe_ttl2 = hop2['probe_ttl']
                                if addr1 != addr2 and probe_ttl1 != probe_ttl2:
                                    distqueue.put((addr1, addr2), 1 if probe_ttl2 - probe_ttl1 == 1 else -1)
    ofilename = os.path.splitext(os.path.basename(filename))[0]
    if addresses_output:
        write_addresses(os.path.join(addresses_output, ofilename + '.txt'), addresses)
    if adjacencies_output:
        write_adjacencies(os.path.join(adjacencies_output, ofilename + '.csv'), adjacencies)
    if dest_pairs_output:
        write_destpairs(os.path.join(dest_pairs_output, ofilename + '.csv'), dest_pairs)
    if distances_output:
        write_distances(os.path.join(distances_output, ofilename + '.csv'), distances)
    # if addrqueue:
    #     addrqueue.put(addresses)
    # if adjqueue:
    #     adjqueue.put(adjacencies)
    # if dpqueue:
    #     dpqueue.put(dest_pairs)
    # if distqueue:
    #     distqueue.put(distances)
    # if testqueue:
    #     testqueue.put(addresses)


def parallel_init(addrq, adjq, dpq, distq, kargs):
    global ip2as, addrqueue, adjqueue, dpqueue, distqueue
    as2org = AS2Org(include_potaroo=False, **kargs)
    bgp = BGP(**kargs)
    ip2as = rt.default_routing_table(as2org=as2org, bgp=bgp, **kargs)
    addrqueue = addrq
    adjqueue = adjq
    dpqueue = dpq
    distqueue = distq


def combine_addresses(queue, filename):
    addresses = set()
    while True:
        newaddrs = queue.get()
        if newaddrs == DONE:
            try:
                write_addresses(filename, addresses)
            finally:
                break
        addresses.add(newaddrs)


def combine_adjacencies(queue, filename):
    adjacencies = set()
    while True:
        newadj = queue.get()
        if newadj == DONE:
            try:
                write_adjacencies(filename, adjacencies)
            finally:
                break
        adjacencies.update(newadj)


def combine_destpairs(queue, filename):
    destpairs = set()
    while True:
        newdp = queue.get()
        if newdp == DONE:
            try:
                write_destpairs(filename, destpairs)
            finally:
                break
        destpairs.update(newdp)


def combine_distances(queue, filename):
    distances = Counter()
    while True:
        newdists = queue.get()
        if newdists == DONE:
            try:
                write_distances(filename, distances)
            finally:
                break
        distances.update(newdists)


def parse_files(files, pool):
    pb = Progress(len(files), 'Reading traceroute files')
    if pool:
        for _ in pb.iterator(pool.imap_unordered(process_trace_file, files)):
            pass
        addrqueue.put(DONE)
        adjqueue.put(DONE)
        dpqueue.put(DONE)
        distqueue.put(DONE)
    else:
        for addrs, adjs, dps, dists in pb.iterator(map(process_trace_file, files)):
            pass


def write_addresses(filename, addresses):
    with open(filename, 'w') as f:
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


def write_distances(filename, distances):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(Distance._fields)
        writer.writerows([[hop1, hop2, num] for (hop1, hop2), num in distances.items()])


def main():
    global addrqueue, adjqueue, dpqueue, distqueue, addresses_output, adjacencies_output, dest_pairs_output, distances_output
    parser = ArgumentParser()
    parser.add_argument('-a', '--adjacencies', dest='adjacencies', required=True, help='Adjacency output file.')
    parser.add_argument('-A', '--adj-per-file', dest='adj_per_file', help='Adjacency output directory.')
    parser.add_argument('-b', '--addresses', dest='addresses', help='Addresses output file.')
    parser.add_argument('-B', '--addr-per-file', dest='addr_per_file', help='Addresses output directory.')
    parser.add_argument('-d', '--destpairs', dest='destpairs', help='Dest pairs output file.')
    parser.add_argument('-D', '--dp-per-file', dest='dp_per_file', help='Dest pairs output directory.')
    parser.add_argument('-e', '--distances', dest='distances', help='Distances between interfaces.')
    parser.add_argument('-E', '--dist-per-file', dest='dist_per_file', help='Distances between interfaces directory.')
    parser.add_argument('-g', '--day', dest='day', type=int, help='Day.')
    parser.add_argument('-f', '--files', dest='files', help='Unix-style file regex.')
    parser.add_argument('-m', '--month', dest='month', type=int, help='Month.')
    parser.add_argument('-r', '--rir', action='store_true', help='Use RIR delegations to fill in missing prefixes.')
    parser.add_argument('-y', '--year', dest='year', type=int, help='Year.')
    parser.add_argument('-p', '--poolsize', dest='poolsize', default=-1, type=int, help='Number of parallel processes.')
    parser.add_argument('-c', '--first-cycle', dest='first_cycle', type=int, help='First cycle number for the traceroutes.')
    parser.add_argument('-z', '--last-cycle', dest='last_cycle', type=int, help='Last cycle number for the traceroutes')
    parser.add_argument('--prefixes', dest='prefixes', help='BGP prefix file to use.')
    args = parser.parse_args()
    if not args.files:
        fregex = 'caida/traces/team-*/daily/*/cycle-*/*{{{:06d}..{:06d}}}.*'.format(args.first_cycle, args.last_cycle)
    else:
        fregex = args.files
    files = list(ls(fregex))
    log.info('Number of files {:,d}'.format(len(files)))
    kargs = dict(year=args.year, month=args.month, day=args.day, prefixes=args.prefixes, rir=args.rir)
    pb = Progress(len(files), 'Reading traceroute files')
    addrqueue = Queue(maxsize=args.poolsize)
    adjqueue = Queue(maxsize=args.poolsize)
    dpqueue = Queue(maxsize=args.poolsize)
    distqueue = Queue(maxsize=args.poolsize)
    addresses_output = args.addr_per_file
    adjacencies_output = args.adj_per_file
    dest_pairs_output = args.dp_per_file
    distances_output = args.dist_per_file
    addrproc = Process(target=combine_addresses, args=(addrqueue, args.addresses))
    addrproc.start()
    adjproc = Process(target=combine_addresses, args=(addrqueue, args.adjacencies))
    adjproc.start()
    dpproc = Process(target=combine_addresses, args=(addrqueue, args.destpairs))
    dpproc.start()
    distproc = Process(target=combine_addresses, args=(addrqueue, args.distances))
    distproc.start()
    if args.poolsize >= 0:
        with multiprocessing.Pool(args.poolsize, parallel_init, (kargs,)) as pool:
            for _ in pb.iterator(pool.imap_unordered(process_trace_file, files)):
                pass
    else:
        parallel_init(kargs)
        for _ in pb.iterator(map(process_trace_file, files)):
            pass
    addrqueue.put(DONE)
    adjqueue.put(DONE)
    dpqueue.put(DONE)
    distqueue.put(DONE)
    addrproc.join()
    adjproc.join()
    dpproc.join()
    distproc.join()
    print('Cleaning up...', file=stderr)


if __name__ == '__main__':
    main()
