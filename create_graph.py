import json
from collections import Counter
from multiprocessing.pool import Pool

from typing import Set, Tuple, Dict, List

from as2org import AS2Org
from bgp.routing_table import RoutingTable
from graph.hybrid_graph import HybridGraph
from traceroute.atlas_trace import AtlasTrace
from traceroute.hop import Hop
from traceroute.output_type import OutputType
from traceroute.warts import Warts
from traceroute.warts_trace import WartsTrace
from utils.progress import Progress
from utils.utils import File2


NEXTHOP = 1
ECHO = 2
MULTI = 3
NONE = 0
BFORWARD = 1
BBACKWARD = 2
DOUBLE = 3

ip2as: RoutingTable = None
as2org: AS2Org = None
pairs: Set[Tuple[str, str]] = None
basns: Dict[str, Set[int]] = None
aasns: Dict[str, Set[int]] = None
marked: Set[str] = None


class Parser:

    def __init__(self, filename, output_type):
        self.filename = filename
        self.output_type = output_type
        self.addrs = set()
        self.dps = set()
        self.dists = Counter()
        self.edges = set()

    def __iter__(self):
        if self.output_type == OutputType.warts:
            with Warts(self.filename, json=True) as f:
                for j in f:
                    if j['type'] == 'trace':
                        yield WartsTrace(j, ip2as=ip2as)
        elif self.output_type == OutputType.atlas:
            with File2(self.filename) as f:
                for j in map(json.loads, f):
                    yield AtlasTrace(j, ip2as=ip2as)

    @staticmethod
    def compute_dist(x: Hop, y: Hop, z: Hop = None):
        distance = y.ttl - x.ttl
        if y.qttl == 0:
            if z and (y.addr == z.addr or y.reply_ttl - z.reply_ttl == (z.ttl - y.ttl) - 1):
                distance -= y.qttl - x.qttl
        elif y.qttl > 1:
            if y.icmp_type == 3 and y.qttl - x.qttl >= y.ttl - x.ttl:
                distance -= y.qttl - x.qttl
        if distance > 1:
            distance = 2
        elif distance < 1:
            distance = -1
        return distance

    def parseone(self, trace):
        self.addrs.update(h.addr for h in trace.allhops if not h.private)
        numhops = len(trace.hops)
        if numhops == 0:
            return
        dest_asn = trace.dst_asn
        if dest_asn > 0:
            self.dps.update((y.addr, dest_asn) for y in trace.hops if y.icmp_type != 0)
        for i in range(numhops - 1):
            x = trace.hops[i]
            y = trace.hops[i+1]
            z = trace.hops[i+2] if i < numhops - 2 else None
            distance = self.compute_dist(x, y, z)
            # self.dists[(x.addr, y.addr)] += 1 if distance == 1 else -1
            if y.addr in marked:
                if x.asn in basns[y.addr]:
                    self.edges.add((x.addr, y.addr, distance, y.icmp_type, BFORWARD))
                elif z:
                    if (y.addr, z.addr) not in pairs:
                        if z.asn in aasns[y.addr]:
                            self.edges.add((x.addr, y.addr, distance, y.icmp_type, BFORWARD))
                    if i < numhops - 3:
                        zz = trace.hops[i+3]
                        if zz.asn in aasns[y.addr]:
                            self.edges.add((x.addr, y.addr, distance, y.icmp_type, BFORWARD))
            elif x.addr in marked:
                if (x.addr, y.addr) in pairs:
                    self.edges.add((x.addr, y.addr, 1, 11, BBACKWARD))
                else:
                    if i > 0:
                        w = trace.hops[i-1]
                        if w.asn in basns[x.addr]:
                            self.edges.add((x.addr, y.addr, distance, y.icmp_type, BBACKWARD))
                    if y.asn in aasns[x.addr]:
                        self.edges.add((x.addr, y.addr, distance, y.icmp_type, DOUBLE))
            else:
                self.edges.add((x.addr, y.addr, distance, y.icmp_type, NONE))

    def parse(self):
        for trace in self:
            self.parseone(trace)


def worker(args):
    filename, output_type = args
    parser = Parser(filename, output_type)
    parser.parse()
    return parser.addrs, parser.edges, parser.dps, parser.dists


def create_graph(files: List[str], graph: HybridGraph, poolsize):
    addrs = set()
    edges = set()
    dps = set()
    dists = Counter()
    pb = Progress(len(files), 'Creating graph', callback=lambda: 'Addrs {:,d} Edges {:,d} DPs {:,d}'.format(len(addrs), len(edges), len(dps)))
    with Pool(poolsize) as pool:
        for newaddrs, newedges, newdps, newdists in pb.iterator(pool.imap_unordered(worker, files)):
            for addr in newaddrs:
                if addr not in addrs:
                    asn = ip2as[addr]
                    org = as2org[asn]
                    graph.add_interface(addr, asn, org, 0)
            addrs.update(newaddrs)
            for edge in newedges:
                if edge not in edges:
                    x, y, distance, icmp_type, special = edge
                    graph.add_edge(x, y, distance, icmp_type, special=special)
            edges.update(newedges)
            for dp in newdps:
                if dp not in dps:
                    addr, dest_asn = dp
                    graph.add_dest(addr, dest_asn)
            dps.update(newdps)
