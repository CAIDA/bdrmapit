from argparse import ArgumentParser
from collections import Counter
from itertools import combinations
from sys import stderr

import numpy as np
import pandas as pd
from logging import getLogger, INFO

from multiprocessing import Queue, Process

import bgp.routing_table as rt
from as2org import AS2Org
from bgp.bgp import BGP
from traceroute.warts import Warts
from utils.progress import Progress
from utils.utils import ls

log = getLogger()
log.setLevel(INFO)

ip2as = None


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


def process_trace_file(jobs, output, status):
    addresses = set()
    adjacencies = set()
    destpairs = set()
    distances = Counter()
    while True:
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
                                qttl1, qttl2 = qttl2, hop2.get('icmp_q_ttl', 1)
                                addr1, addr2 = addr2, hop2['addr']
                                issuspicious = hop2['probe_ttl'] > suspicious
                                if hop2['asn'] > -2:
                                    destpairs.add((addr2, dest_asn, issuspicious, loop))
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
                                    adjacencies.add((addr1, addr2, link_type, hop1['private'], issuspicious, hop1['icmp_type']))
                            for hop1, hop2 in combinations(hopslist, 2):
                                addr1 = hop1['addr']
                                addr2 = hop2['addr']
                                probe_ttl1 = hop1['probe_ttl']
                                probe_ttl2 = hop2['probe_ttl']
                                if addr1 != addr2 and probe_ttl1 != probe_ttl2:
                                    distances[(addr1, addr2)] += 1 if probe_ttl2 - probe_ttl1 == 1 else -1
        status.put(1)
    output.put((addresses, adjacencies, destpairs, distances))


def main():
    global ip2as
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
    as2org = AS2Org(include_potaroo=False, **kargs)
    bgp = BGP(**kargs)
    ip2as = rt.default_routing_table(as2org=as2org, bgp=bgp, **kargs)
    jobs = Queue()
    output = Queue()
    status = Queue()
    procs = [Process(target=process_trace_file, args=(jobs, output, status)) for _ in range(args.pool)]
    for filename in files:
        jobs.put(filename)
    pb = Progress(len(files), 'Reading traceroute files')
    status.get()
    for _ in pb.iterator(range(len(files))):
        status.get()
    addresses = set()
    adjacencies = set()
    destpairs = set()
    distances = Counter()
    pb = Progress(len(procs), 'Retrieving results')
    for _ in pb.iterator(range(len(procs))):
        addrs, adjs, dps, dists = output.get()
        addresses.update(addrs)
        adjacencies.update(adjs)
        destpairs.update(dps)
        distances.update(dists)
    print('Cleaning up...', file=stderr)


if __name__ == '__main__':
    main()