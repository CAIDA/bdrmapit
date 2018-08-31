#!/usr/bin/env python
import os.path
import sqlite3
from argparse import ArgumentParser
from collections import Counter, namedtuple
from multiprocessing.pool import Pool
from os import makedirs
from os.path import basename

import algorithm as alg
import last_hop as lh
from as2org import AS2Org
from bdrmapit import opendb
from bgp.bgp import BGP
from bgp.routing_table import RoutingTable
from create_objs import CreateObjs
from graph.abstract_graph import AbstractGraph
from graph.bdrmapit import Bdrmapit
from graph.hybrid_graph import HybridGraph
from graph.hybrid_graph_view import HybridGraphView
from traceroute.abstract_trace import AbstractTrace
from traceroute.atlas_trace import AtlasTrace
from traceroute.output_type import OutputType
from traceroute.parser import Parser
from traceroute.warts_trace import WartsTrace
from updates_dict import Updates
from utils.progress import Progress
from utils.utils import File2

ip2as: RoutingTable = None
as2org: AS2Org = None
bgp: BGP = None
output_dir: str = None


Result = namedtuple('Result', ['addr', 'router', 'asn', 'conn_asn', 'org', 'conn_org', 'iasn', 'iorg', 'utype', 'itype', 'rtype'])


class SingleTrace:

    def __init__(self, traceroute, ip2as: RoutingTable, as2org: AS2Org, bgp: BGP, output_type: OutputType = OutputType.warts):
        self.ip2as = ip2as
        self.as2org = as2org
        self.bgp = bgp
        self.addrs = set()
        self.adjs = set()
        self.dps = set()
        self.dists = Counter()
        self.g = None
        if output_type == OutputType.warts:
            self.trace = WartsTrace(traceroute, ip2as=ip2as)
        elif OutputType.atlas:
            self.trace = AtlasTrace(traceroute, ip2as=ip2as)

    def parseone(self):
        self.addrs.update((h.addr, h.num) for h in self.trace.allhops if not h.private)
        dest_asn = self.trace.dst_asn
        numhops = len(self.trace.hops)
        if numhops == 0:
            return
        i = 0
        y = self.trace.hops[0]
        if not y.private:
            self.dps.add((y.addr, dest_asn, y.icmp_type == 0))
        while i < numhops - 1:
            i += 1
            x, y, z = y, self.trace.hops[i], (self.trace.hops[i + 1] if i < numhops - 1 else None)
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
            self.adjs.add((x.addr, y.addr, distance, y.icmp_type))
            self.dps.add((y.addr, dest_asn, y.icmp_type == 0))
            self.dists[(x.addr, y.addr)] += 1 if distance == 1 else -1

    def create_graph(self, g: AbstractGraph = None, quiet=False):
        if quiet:
            Progress.set_output(False)
        dps = {(addr, asn) for addr, asn, echo in self.dps if not echo}
        if g is not None:
            self.g = HybridGraphView(g)
        else:
            self.g = HybridGraph()
        co = CreateObjs(self.g, self.ip2as, self.as2org, self.bgp)
        co.read_addresses(self.addrs)
        self.g.finalize_routers()
        co.create_graph(self.adjs, self.dists)
        co.destpairs(dps=dps)
        self.g.set_routers_interfaces()

    def singlerun(self, lhupdates: Updates = None, rupdates: Updates = None, iupdates: Updates = None, quiet=True):
        if quiet:
            Progress.set_output(False)
        bdrmapit = Bdrmapit(self.g, self.as2org, self.bgp, lhupdates=lhupdates)
        lh.annotate_lasthops(bdrmapit, routers=self.g.routers_nosucc)
        rupdates, iupdates = alg.graph_refinement(bdrmapit, self.g.routers_succ, self.g.interfaces_pred, rupdates=rupdates, iupdates=iupdates, create_changed=True)
        values = []
        for hop in self.trace.hops:
            router = self.g.name_router[hop.addr]
        # for router in bdrmapit.graph.routers:
            if bdrmapit.graph.rnexthop[router]:
                rtype = 'n'
            elif bdrmapit.graph.recho[router]:
                rtype = 'e'
            elif bdrmapit.graph.rmulti[router]:
                rtype = 'm'
            else:
                rtype = 'l'
            rasn, rorg, utype = bdrmapit.lhupdates.get(router, rupdates[router])
            for interface in bdrmapit.graph.router_interfaces[router]:
                if interface.org == rorg or interface.asn == 0:
                    iasn, iorg, itype = iupdates[interface]
                    if iasn == -1:
                        iasn, iorg, itype = interface.asn, interface.org, -1
                else:
                    iasn, iorg, itype = interface.asn, interface.org, -2
                d = Result(interface.address, router.name, rasn, iasn, rorg, iorg, interface.asn, interface.org, utype, itype, rtype)
                values.append(d)
        return values

    def run(self, g: AbstractGraph = None, lhupdates: Updates = None, rupdates: Updates = None, iupdates: Updates = None, quiet=True):
        if quiet:
            Progress.set_output(False)
        self.parseone()
        self.create_graph(g)
        return self.singlerun(lhupdates=lhupdates, rupdates=rupdates, iupdates=iupdates)


class FileParse:

    def __init__(self, filename: str, output_type: OutputType, ip2as: RoutingTable, as2org: AS2Org, bgp: BGP):
        self.ip2as = ip2as
        self.as2org = as2org
        self.bgp = bgp
        self.values = []
        self.parser = Parser(filename, output_type, ip2as)

    def parse_separate(self):
        pb = Progress(message='Parsing the traceroutes', increment=1000)
        Progress.set_output(False)
        for trace in pb.iterator(self.parser):
            if len(trace.hops) >= 5:
                self.singlerun(trace)
                self.parser.reset()

    def singlerun(self, trace: AbstractTrace):
        self.parser.parseone(trace)
        dps = {(addr, asn) for addr, asn, echo in self.parser.dps if not echo}
        g = HybridGraph()
        co = CreateObjs(g, self.ip2as, self.as2org, self.bgp)
        co.read_addresses(self.parser.addrs)
        g.router_interfaces.finalize()
        co.create_graph(self.parser.adjs, self.parser.dists)
        co.destpairs(dps=dps)
        g.set_routers_interfaces()
        bdrmapit = Bdrmapit(g, self.as2org, self.bgp, step=0)
        lh.annotate_lasthops(bdrmapit, routers=g.routers_nosucc)
        rupdates, iupdates = alg.graph_refinement(bdrmapit, g.routers_succ, g.interfaces_pred)
        for router in bdrmapit.graph.routers:
            if router in bdrmapit.graph.rnexthop:
                rtype = 'n'
            elif router in bdrmapit.graph.recho:
                rtype = 'e'
            elif router in bdrmapit.graph.rmulti:
                rtype = 'm'
            else:
                rtype = 'l'
            rasn, rorg, utype = bdrmapit.lhupdates.get(router, rupdates[router])
            for interface in bdrmapit.graph.router_interfaces[router]:
                if interface.org == rorg or interface.asn == 0:
                    iasn, iorg, itype = iupdates[interface]
                    if iasn == -1:
                        iasn, iorg, itype = interface.asn, interface.org, -1
                else:
                    iasn, iorg, itype = interface.asn, interface.org, -2
                d = (interface.address, router.name, rasn, iasn, rorg, iorg, interface.asn, interface.org, utype, itype, rtype)
                self.values.append(d)

    def save(self, filename, remove=False, chunksize=10000):
        con: sqlite3.Connection = opendb(filename, remove=remove)
        query = 'INSERT INTO annotation (addr, router, asn, conn_asn, org, conn_org, iasn, iorg, utype, itype, rtype) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
        for i in range(0, len(self.values) + 1, chunksize):
            cur = con.cursor()
            cur.executemany(query, self.values[i:i+chunksize])
            cur.close()
            con.commit()
        con.close()


def run(filename: str):
    output = basename(filename).rpartition('.')[0]
    output = '{}.db'.format(output)
    output = os.path.join(output_dir, output)
    fp = FileParse(filename, OutputType.warts, ip2as, as2org, bgp)
    fp.parse_separate()
    fp.save(output, remove=True)


def main():
    global ip2as, as2org, bgp, output_dir
    parser = ArgumentParser()
    parser.add_argument('-i', '--ip2as', required=True, help='BGP prefix file regex to use.')
    parser.add_argument('-a', '--as2org', required=True, help='AS-to-Org mappings in the standard CAIDA format.')
    parser.add_argument('-r', '--rels', required=True, help='AS relationship file in the standard CAIDA format.')
    parser.add_argument('-c', '--cone', required=True, help='AS customer cone file in the standard CAIDA format.')
    parser.add_argument('-p', '--parallel', type=int)
    parser.add_argument('-o', '--output', required=True, help='Results database file.')
    parser.add_argument('-R', '--remove', action='store_true', help='Remove file if it exists')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--filename', help='Traceroute filename')
    group.add_argument('-F', '--file-list')
    args = parser.parse_args()
    bgp = BGP(args.rels, args.cone)
    as2org = AS2Org(args.as2org, include_potaroo=False)
    ip2as = RoutingTable.ip2as(args.ip2as)
    output_dir = args.output
    makedirs(output_dir, exist_ok=True)
    if args.filename:
        filenames = [args.filename]
    else:
        with File2(args.file_list) as f:
            filenames = [l.strip() for l in f]
    if not args.parallel or len(filenames) == 1:
        for filename in filenames:
            run(filename)
    else:
        pb = Progress(len(filenames), 'Running bdrmapIT single traceroute')
        Progress.set_output(False)
        with Pool(args.parallel) as pool:
            for _ in pb.iterator(pool.imap_unordered(run, filenames)):
                pass


if __name__ == '__main__':
    main()
