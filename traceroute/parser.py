import csv
import multiprocessing
from argparse import ArgumentParser
from collections import namedtuple, Counter
import os
from itertools import combinations

import numpy as np
import pandas as pd
import re

from logging import getLogger, INFO

import bgp.routing_table as rt
from as2org import AS2Org
from bgp.bgp import BGP
from utils.progress import Progress
from traceroute.warts import Warts
from utils.utils import ls

log = getLogger()
log.setLevel(INFO)

ip2as = None
addresses_output = None
adjacencies_output = None
dest_pairs_output = None
distances_output = None
error = False
complete = []
record_vp = False
trace_regex = {
    '03-2016': 'caida/traces/team-*/daily/2016/cycle-*/*c0046{02..31}.*',
    '09-2016': 'caida/traces/team-*/daily/2016/cycle-*/*{5013..5050}.*',
    '02-2017': 'caida/traces/team-*/daily/2017/cycle-*/*{5397..5431}.*'
}
Adjacency = namedtuple('Adjacency', ['Hop1', 'Hop2', 'Distance', 'Private', 'Suspicious', 'Type', 'Code'])
DestPair = namedtuple('DestPair', ['Interface', 'DestASN', 'Suspicious', 'SecondLast', 'Last', 'StopReason', 'StopData'])
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
    real1 = ttl1 + (1 - qttl1)
    real2 = ttl2 + (1 - qttl2)
    return real2 - real1


def calculate_rttl_distance(hop1, hop2):
    return hop1['reply_ttl'] - hop2['reply_ttl']


def parse_adjacencies(hopslist, suspicious):
    for hop1, hop2 in zip(hopslist, hopslist[1:]):
        distance = calculate_distance(hop1, hop2)
        ttl = hop2['probe_ttl']
        issuspicious = ttl > suspicious
        icmp_type = hop2['icmp_type']
        icmp_code = hop2['icmp_code']
        yield Adjacency(hop1['addr'], hop2['addr'], distance, hop1['private'], issuspicious, icmp_type, icmp_code)


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


def extract_dest_pairs(hopslist, dest_asn, stop_reason, stop_data, suspicious, original_last_hop):
    for hop in hopslist:
        if hop['asn'] > -2:
            ttl = hop['probe_ttl']
            issuspicious = ttl > suspicious
            second_last = ttl == original_last_hop - 1
            last = ttl == original_last_hop
            if not second_last and not last:
                sr = np.nan
                sd = np.nan
            else:
                sr = stop_reason
                sd = stop_data
            yield DestPair(hop['addr'], dest_asn, issuspicious, second_last, last, sr, sd)


def extract_distances(hopstlist):
    distances = {}
    for hop1, hop2 in combinations(hopstlist, 2):
        addr1 = hop1['addr']
        addr2 = hop2['addr']
        probe_ttl1 = hop1['probe_ttl']
        probe_ttl2 = hop2['probe_ttl']
        if addr1 != addr2 and probe_ttl1 != probe_ttl2:
            distances[(addr1, addr2)] = 1 if probe_ttl2 - probe_ttl1 == 1 else -1
    return distances


def process_trace_file(filename):
    addresses = set()
    adjacencies = set()
    dest_pairs = set()
    distances = Counter()
    with Warts(filename, json=True) as f:
        for j in f:
            if j['type'] == 'trace':
                stop_reason = j['stop_reason']
                stop_data = j['stop_data']
                original_hopslist = j.get('hops')
                if original_hopslist:
                    original_last_hop = original_hopslist[-1]
                    # original_last_ttl = original_last_hop['probe_ttl']
                    original_last_distance = hop_distance(original_last_hop)
                    if addresses_output:
                        addresses.update(hop['addr'] for hop in original_hopslist)
                    if adjacencies_output or dest_pairs_output or distances_output:
                        hopslist = extract_trace(original_hopslist, j['hop_count'])
                        hopslist = remove_private(hopslist)
                        hopslist = remove_loops(hopslist)
                        if hopslist:
                            dst = j['dst']
                            dest_asn = ip2as[dst]
                            suspicious = suspicious_hop(hopslist, dest_asn)
                            if adjacencies_output:
                                adjacencies.update(parse_adjacencies(hopslist, suspicious))
                            if dest_pairs_output:
                                dest_pairs.update(extract_dest_pairs(hopslist, dest_asn, stop_reason, stop_data, suspicious, original_last_distance))
                            if distances_output:
                                distances.update(extract_distances(hopslist))
        ofilename = os.path.splitext(os.path.basename(filename))[0]
        if addresses_output and addresses:
            write_addresses(os.path.join(addresses_output, ofilename + '.txt'), addresses)
        if adjacencies_output and adjacencies:
            write_adjacencies(os.path.join(adjacencies_output, ofilename + '.csv'), adjacencies)
        if dest_pairs_output and dest_pairs:
            write_destpairs(os.path.join(dest_pairs_output, ofilename + '.csv'), dest_pairs)
        if distances_output and distances:
            write_distances(os.path.join(distances_output, ofilename + '.csv'), distances)
    return None


def parallel_init(addresses, adjacencies, destpairs, distances, kargs, rt_obj=None):
    global ip2as
    global addresses_output, adjacencies_output, dest_pairs_output, distances_output
    if rt_obj is None:
        as2org = AS2Org(include_potaroo=False, **kargs)
        bgp = BGP(**kargs)
        ip2as = rt.default_routing_table(as2org=as2org, bgp=bgp, **kargs)
    else:
        ip2as = rt_obj
    addresses_output = addresses
    adjacencies_output = adjacencies
    dest_pairs_output = destpairs
    distances_output = distances


def parse_files(files, pool):
    pb = Progress(len(files), 'Reading traceroute files')
    if pool:
        results = pool.imap_unordered(process_trace_file, files)
    else:
        results = map(process_trace_file, files)
    for filename, _ in pb.iterator(zip(files, results)):
        complete.append(filename)


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
    parser = ArgumentParser()
    parser.add_argument('-a', '--adjacencies', dest='adjacencies', help='Adjacency output file.')
    parser.add_argument('-b', '--addresses', dest='addresses', help='Addresses output file.')
    parser.add_argument('-d', '--destpairs', dest='destpairs', help='Dest pairs output file.')
    parser.add_argument('-e', '--distances', dest='distances', help='Distances between interfaces.')
    parser.add_argument('-D', '--day', dest='day', type=int, help='Day.')
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
    if args.poolsize >= 0:
        pool = multiprocessing.Pool(args.poolsize, parallel_init, (args.addresses, args.adjacencies, args.destpairs, args.distances, kargs))
    else:
        parallel_init(args.addresses, args.adjacencies, args.destpairs, args.distances, kargs)
        pool = None
    parse_files(files, pool)
    if pool:
        pool.close()


if __name__ == '__main__':
    main()
