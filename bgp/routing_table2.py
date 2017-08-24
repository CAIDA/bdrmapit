from argparse import ArgumentParser
from collections import Counter, defaultdict
from glob import glob
from itertools import chain
import os.path
import sys

import pandas as pd
import re
from radix import Radix

import rir as rirfuncts
from utils.utils import infer_compression, max_num, File2, unique_everseen, decompresses_or_first

PRIVATE4 = ['0.0.0.0/8', '10.0.0.8/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16', '172.16.0.0/12',
            '192.0.0.0/24', '192.0.2.0/24', '192.31.196.0/24', '192.52.193.0/24', '192.88.99.0/24', '192.168.0.0/16',
            '192.175.48.0/24', '198.18.0.0/15', '198.51.100.0/24', '203.0.113.0/24', '240.0.0.0/4',
            '255.255.255.255/32']
PRIVATE6 = ['::1/128', '::/128', '::ffff:0:0/96', '64:ff9b::/96', '100::/64', '2001::/23', '2001::/32', '2001:1::1/128',
            '2001:2::/48', '2001:3::/32', '2001:4:112::/48', '2001:5::/32', '2001:10::/28', '2001:20::/28',
            '2001:db8::/32', '2002::/16', '2620:4f:8000::/48', 'fc00::/7', 'fe80::/10']
MULTICAST4 = '224.0.0.0/3'
MULTICAST6 = 'FF00::/8'
path = __file__.rpartition('/')[0]
bgp_files = {
    '08-2015': os.path.join(path, '../caida/prefix2as/routeviews-rv2-20150820-1200.pfx2as.gz'),
    '03-2016': os.path.join(path, '../caida/prefix2as/routeviews-rv2-20160320-1200.pfx2as.gz'),
    # '03-2016': os.path.join(path, 'prefixes/bgp.2016.03.28.txt'),
    '05-2016': 'caida/prefix2as/routeviews-rv2-20160320-1200.pfx2as.gz',
    '09-2016': os.path.join(path, '../caida/prefix2as/routeviews-rv2-20160918-1200.pfx2as.gz'),
    '02-2017': os.path.join(path, '../caida/prefix2as/routeviews-rv2-20170207-1400.pfx2as.gz')
}
split = re.compile('_|,')


class RoutingTable(Radix):
    @classmethod
    def private(cls, inet='both'):
        rt = cls()
        rt.add_private(inet=inet, remove=False)
        rt.add_default()
        return rt

    def __init__(self):
        super().__init__()

    def __getitem__(self, item):
        return self.search_best(item).data['asn']

    def __setitem__(self, key, value):
        self.add(key).data['asn'] = value

    def add_default(self):
        self.add_prefix(0, '0.0.0.0/0')

    def add_ixp(self, network=None, masklen=None, packed=None, remove=True):
        if remove:
            covered = self.search_covered(network, masklen) if network and masklen else self.search_covered(network)
            for node in covered:
                try:
                    self.delete(node.prefix)
                except KeyError:
                    pass
        self.add_prefix(-1, network, masklen, packed)

    def add_prefix(self, asn, network=None, masklen=None, packed=None):
        if network and masklen:
            node = self.add(network, masklen)
        elif network:
            node = self.add(network)
        node.data['asn'] = asn

    def add_multicast(self, inet='both', remove=True):
        prefixes = []
        if inet == 'ipv4' or 'both':
            prefixes.append(MULTICAST4)
        if inet == 'ipv6' or 'both':
            prefixes.append(MULTICAST6)
        for prefix in prefixes:
            if remove:
                for node in self.search_covered(prefix):
                    self.delete(node.prefix)
            self.add_prefix(-3, prefix)

    def add_private(self, inet='both', remove=True):
        if inet == 'both':
            prefixes = chain(PRIVATE4, PRIVATE6)
        elif inet == 'ipv4':
            prefixes = PRIVATE4
        elif inet == 'ipv6':
            prefixes = PRIVATE6
        else:
            raise Exception('Unknown INET {}'.format(inet))
        for prefix in prefixes:
            if remove:
                for node in self.search_covered(prefix):
                    self.delete(node.prefix)
            self.add_prefix(-2, prefix)

    def add_rir(self, rir, ixp_asns):
        rirrows = []
        for address, prefixlen, asn in rir:
            if asn not in ixp_asns:
                if not self.search_covering(address, prefixlen):
                    rirrows.append((address, prefixlen, asn))
        for address, prefixlen, asn in rirrows:
            self.add_prefix(asn, address, prefixlen)

    def isglobal(self, address):
        return self[address] >= -1


def valid(asn):
    return 0 < asn < 64496 or 131071 < asn < 4200000000


def determine_asn(address, prefixlen, asns, rir=None, bgp=None, as2org=None):
    asns = list(map(int, split.split(asns)))
    # print(asns)
    if len(asns) == 1:
        return asns[0]
    asns = [asn for asn in unique_everseen(asns) if valid(asn)]
    # print(asns)
    if not asns:
        return 0
    if len(asns) == 1:
        return asns[0]
    if as2org is not None:
        if len({as2org[asn] for asn in asns}) == 1:
            return asns[0]
    # if rir is not None:
    #     rasn = rir['{}/{}'.format(address, prefixlen)]
    #     if rasn > 0:
    #         for asn in asns:
    #             if rasn == asn:
    #                 return asn
    if bgp is not None:
        for asn in asns:
            if all(asn in bgp.cone(other) for other in asns if other != asn):
                return asn
        mins = max_num(asns, key=lambda x: -bgp.conesize(x))
    #     if len(mins) == 1:
    #         return mins[0]
    #     mins = max_num(asns, key=lambda x: bgp.conesize(x))
        # print(mins)
        if len(mins) == 1:
            return mins[0]
    else:
        mins = asns
    # print(address, prefixlen, mins)
    return mins[0]


def read_prefixes(filename, rir=None, bgp=None, as2org=None):
    with File2(filename) as f:
        for line in f:
            if line[0] != '#':
                address, prefixlen, asns = line.split()
                prefixlen = int(prefixlen)
                asn = determine_asn(address, prefixlen, asns, rir=rir, bgp=bgp, as2org=as2org)
                if asn:
                    yield address, prefixlen, asn


def create_routing_table(prefixes, ixp_prefixes=None, ixp_asns=None, rir=None, bgp=None, as2org=None):
    if ixp_prefixes is not None and not isinstance(ixp_prefixes, pd.DataFrame):
        ixp_prefixes = pd.read_csv(ixp_prefixes, comment='#', index_col=0)
    else:
        ixp_prefixes = pd.DataFrame([])
    if ixp_asns is not None and not isinstance(ixp_asns, pd.DataFrame):
        ixp_asns = pd.read_csv(ixp_asns, comment='#', index_col=0)
    else:
        ixp_asns = []
    rt = RoutingTable()
    bgp_ixp = []
    for address, prefixlen, asn in read_prefixes(prefixes, bgp=bgp, as2org=as2org) if isinstance(prefixes, str) else prefixes:
        if asn not in ixp_asns:
            rt.add_prefix(asn, address, prefixlen)
        else:
            bgp_ixp.append((address, prefixlen))
    if rir is not None:
        rt.add_rir(rir, ixp_asns)
    for address, prefixlen in bgp_ixp:
        rt.add_ixp(address, prefixlen)
    for prefix in ixp_prefixes.index:
        rt.add_ixp(prefix)
    rt.add_private()
    rt.add_multicast()
    rt.add_default()
    return rt


def default_routing_table(prefixes=None, ixp_prefixes='ixp_prefixes.csv', ixp_asns='ixp_asns.csv', year=None, month=None, day=None, rir=True, bgp=None, as2org=None, **kargs):
    if prefixes is None:
        prefixes = find_files(year, month, day)
    rir = rirfuncts.all_prefixes(year, month, day) if rir else None
    ip2as = create_routing_table(prefixes, ixp_prefixes, ixp_asns, rir=rir, bgp=bgp, as2org=as2org)
    return ip2as


def find_files(year, month, day, directory='caida/prefix2as'):
    date = '{}{:02d}{:02d}'.format(year, month, day)
    regex = os.path.join(directory, 'routeviews-rv2-{}-*.pfx2as*'.format(date))
    # print(regex)
    rels = glob(regex)
    rel = decompresses_or_first(rels)
    return rel


def map_unannounced(filename):
    df = pd.read_csv(filename)
    sp = defaultdict(Counter)
    ep = defaultdict(Counter)
    for row in df[(df.SASN != -2) & (df.EASN != -2)].itertuples(index=False):
        prefix = '{}.0'.format(row.Address.rpartition('.')[0])
        if row.SASN != -1:
            sp[prefix][row.SASN] += 1
        ep[prefix][row.EASN] += 1
    assignments = []
    for address in ep:
        ac = sp[address] + ep[address]
        most_frequent = max_num(ac, key=ac.__getitem__)
        if len(most_frequent) == 1:
            assignments.append([address, 24, most_frequent[0]])
    return assignments


def main():
    parser = ArgumentParser()
    parser.add_argument('-p', '--prefixes', required=True, help='Regex for prefix-to-AS files in the standard CAIDA format.')
    parser.add_argument('-i', '--ixp-prefixes', help='List of IXP prefixes, one per line.')
    parser.add_argument('-I', '--ixp-asns', help='List of IXP ASNs, one per line. Used to identify IXP prefixes in the prefix-to-AS files.')
    parser.add_argument('-r', '--rir', help='RIR extended delegation file regex.')
    parser.add_argument('-R', '--rels', help='AS relationship file in the standard CAIDA format.')
    args = parser.parse_args()
