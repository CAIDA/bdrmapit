#!/usr/bin/env python
import os.path
import sqlite3
from argparse import ArgumentParser
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
from graph.bdrmapit import Bdrmapit
from graph.hybrid_graph import HybridGraph
from traceroute.abstract_trace import AbstractTrace
from traceroute.output_type import OutputType
from traceroute.parser import Parser
from utils.progress import Progress
from utils.utils import File2

ip2as: RoutingTable = None
as2org: AS2Org = None
bgp: BGP = None
output_dir: str = None


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
