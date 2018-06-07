#!/usr/bin/env python
import csv
import re
from argparse import ArgumentParser, FileType

import pandas as pd

import rirparser
from as2org import AS2Org
from bgp.bgp import BGP
from bgp.routing_table import RoutingTable
from utils.utils import read_filenames, unique_everseen, max_num, File2

split = re.compile('[_,]')


def create_routing_table(prefixes, ixp_prefixes=None, ixp_asns=None, rir=None, bgp=None, as2org=None):
    ixp_prefixes = pd.read_csv(ixp_prefixes, comment='#').iloc[:, 0] if ixp_prefixes is not None else []
    ixp_asns = set(pd.read_csv(ixp_asns, comment='#').iloc[:, 0]) if ixp_asns else []
    rt = RoutingTable()
    bgp_ixp = []
    for address, prefixlen, asn in read_prefixes(prefixes, bgp=bgp, as2org=as2org):
        if asn not in ixp_asns:
            try:
                rt.add_prefix(asn, address, prefixlen)
            except OverflowError:
                print(asn, address, prefixlen)
                raise
        else:
            bgp_ixp.append((address, prefixlen))
    if rir is not None:
        for filename in rir:
            rt.add_rir(rirparser.delegations(filename), ixp_asns)
    for address, prefixlen in bgp_ixp:
        rt.add_ixp(address, prefixlen)
    for prefix in ixp_prefixes:
        if prefix:
            try:
                rt.add_ixp(prefix)
            except TypeError:
                print('TypeError:', prefix)
    rt.add_private()
    rt.add_multicast()
    rt.add_default()
    return rt


def determine_asn(asnsstr, bgp=None, as2org=None):
    asns = []
    for asnstr in split.split(asnsstr):
        if asnstr:
            asn = int(asnstr)
            if valid(asn):
                asns.append(asn)
    if len(asns) == 1:
        return asns[0]
    if not asns:
        return 0
    if len(asns) == 1:
        return asns[0]
    if as2org is not None:
        if len({as2org[asn] for asn in asns}) == 1:
            return asns[0]
    if bgp is not None:
        for asn in asns:
            if all(asn in bgp.cone[other] for other in asns if other != asn):
                return asn
        try:
            mins = max_num(asns, key=lambda x: -bgp.conesize[x])
        except OverflowError:
            print(asns)
            raise
        if len(mins) == 1:
            return mins[0]
    else:
        mins = asns
    return mins[0]


def read_prefixes(filename, bgp=None, as2org=None):
    with File2(filename) as f:
        for line in f:
            if line[0] != '#':
                address, prefixlen, asns = line.split()
                prefixlen = int(prefixlen)
                asn = determine_asn(asns, bgp=bgp, as2org=as2org)
                if asn:
                    yield address, prefixlen, asn


def valid(asn):
    return asn != 23456 and 0 < asn < 64496 or 131071 < asn < 4200000000


def main():
    parser = ArgumentParser()
    parser.add_argument('-p', '--prefixes', required=True, help='Regex for prefix-to-AS files in the standard CAIDA format.')
    parser.add_argument('-i', '--ixp-prefixes', help='List of IXP prefixes, one per line.')
    parser.add_argument('-r', '--rir', help='RIR extended delegation file regex.')
    parser.add_argument('-R', '--rels', help='AS relationship file in the standard CAIDA format.')
    parser.add_argument('-c', '--cone', help='AS customer cone file in the standard CAIDA format.')
    parser.add_argument('-o', '--output', type=FileType('w'), default='-', help='Output file.')
    parser.add_argument('-a', '--as2org', help='AS-to-Org mappings in the standard CAIDA format.')
    args = parser.parse_args()
    rir = list(read_filenames(args.rir))
    bgp = BGP(args.rels, args.cone)
    as2org = AS2Org(args.as2org, include_potaroo=False)
    ip2as = create_routing_table(args.prefixes, ixp_prefixes=args.ixp_prefixes, ixp_asns=None, rir=rir, bgp=bgp, as2org=as2org)
    nodes = ip2as.nodes()
    writer = csv.writer(args.output)
    writer.writerow(['prefix', 'asn'])
    writer.writerows([node.prefix, node.data['asn']] for node in nodes)


if __name__ == '__main__':
    main()
