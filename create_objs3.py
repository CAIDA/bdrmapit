import sqlite3
from collections import defaultdict
from sys import stderr
from typing import Iterable, Tuple

from as2org import AS2Org
from bgp.bgp import BGP
from bgp.routing_table import RoutingTable
from graph.abstract_graph import AbstractGraph
from graph.hybrid_graph import HybridGraph
from utils.progress import Progress
from utils.utils import File2


class CreateObjs:
    def __init__(self, g: AbstractGraph, ip2as: RoutingTable, as2org: AS2Org, bgp: BGP, filename: str = None):
        self.con = sqlite3.connect(filename) if filename is not None else None
        self.g = g
        self.ip2as = ip2as
        self.as2org = as2org
        self.bgp = bgp

    def add_address(self, addr, num):
        asn = self.ip2as[addr]
        if asn > -2:
            org = self.as2org[asn]
            self.g.add_interface(addr, asn, org, num)
            return 1
        return 0

    def read_addresses(self, address: Iterable[Tuple[str, int]] = None, increment=1000000):
        i = 0
        if address is None:
            try:
                address = self.con.execute('SELECT addr, num FROM address')
            except sqlite3.OperationalError:
                # print('No num column. Ignoring nums.')
                address = self.con.execute('SELECT addr, 0 FROM address')
        pb = Progress(message='Addresses', increment=increment, callback=lambda: '{:,d}'.format(i))
        for addr, num in pb.iterator(address):
            i += self.add_address(addr, num)

    def alias_resolution(self, filename: str, useall=False, increment=1000000):
        i = 0
        j = 0
        k = 0
        pb = Progress(message='Reading nodes', increment=increment, callback=lambda: 'Routers {:,d} Included {:,d} Added {:,d}'.format(i, j, k))
        with File2(filename) as f:
            for line in pb.iterator(f):
                if line[0] != '#':
                    _, n, *addrs = line.split()
                    if not useall:
                        addresses = [address for address in addrs if address in self.g.address_interface]
                    else:
                        addresses = []
                        for addr in addrs:
                            used = True
                            if addr not in self.g.address_interface:
                                used = self.add_address(addr, 0)
                                k += used
                            if used:
                                addresses.append(addr)
                    if len(addresses) > 1:
                        i += 1
                    nid = n[:-1]
                    router = self.g.add_router(nid)
                    self.g.group_interfaces(router, addresses)
                    j += 1
        self.g.finalize_routers()

    def create_graph(self, edges=None, plusone=None, adjs=None, increment=1000000, filename=None):
        if filename:
            self.con = sqlite3.connect(filename)
        used = 0
        extras = defaultdict(set)
        before = defaultdict(set)
        plusany = defaultdict(bool)
        if plusone is None:
            Progress.message('Reading plus1s', file=stderr)
            plusone = set(self.con.execute('select hop1, hop2 from adjacency WHERE plusone = true'))
            Progress.message('Plus1: {:,d}'.format(len(plusone)))
        if adjs is None:
            Progress.message('Reading distances', file=stderr)
            adjs = set(self.con.execute('select hop1, hop2 from distance where distance > 0'))
        pb = Progress(message='Adding neighbors', increment=increment, callback=lambda: 'Seen {:,d}'.format(len(before)))
        if edges is None:
            edges = self.con.execute('select hop1, hop2, hop3, distance, type from adjacency where distance > 0')
        for h1, h2, h3, dist, icmp_type in pb.iterator(edges):
            reverse = (h1, h2) in plusone
            if h1:
                before[h2, h3].add(h1)
            plusany[h2, h3] |= reverse
            extras[h2, h3].add((dist, icmp_type))
        allplus = {(h1, h2) for (h2, h3), h1s in before.items() if plusany[h2, h3] for h1 in h1s}
        Progress.message('All Plus {:,d}'.format(len(allplus)), file=stderr)
        pb = Progress(len(before), 'Creating edges', increment=increment, callback=lambda: 'Used {:,d}'.format(used))
        for (h2, h3), extra in pb.iterator(extras.items()):
            if plusany[(h2, h3)]:
                special = 2
            elif (h2, h3) in allplus:
                special = 1
            else:
                special = 0
            if (h2, h3) not in adjs:
                adist = 10
            else:
                adist = 0
            for dist, icmp_type in extra:
                used += self.g.add_edge(h2, h3, dist + adist, icmp_type, special=special)
        self.g.finalize_edges()

    def destpairs(self, dps=None, increment=500000):
        used = 0
        pb = Progress(message='Dest pairs', increment=increment, callback=lambda: 'Used {:,d}'.format(used))
        if dps is None:
            query = 'SELECT addr, asn FROM destpair WHERE asn > 0 AND not echo'
            dps = self.con.execute(query)
        for addr, dest_asn in pb.iterator(dps):
            self.g.add_dest(addr, dest_asn)
            used += 1
        self.g.finalize_dests()
        self.g.set_dests(self.as2org, self.bgp)
