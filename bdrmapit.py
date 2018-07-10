#!/usr/bin/env python
import json
import os
import sqlite3
from argparse import ArgumentParser
from multiprocessing.pool import Pool

import algorithm as alg
from create_objs import CreateObjs
import last_hop as lh
from as2org import AS2Org
from bgp.bgp import BGP
from bgp.routing_table import RoutingTable
from graph.bdrmapit import Bdrmapit
from graph.hybrid_graph import HybridGraph
from utils.progress import Progress
import pandas as pd
from collections import namedtuple, defaultdict

Experiment = namedtuple('Experiment', ['db', 'nodes', 'output', 'ip2as', 'as2org', 'rels', 'cone', 'iterations'])


def opendb(filename, remove=False):
    if remove:
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
    directory = os.path.split(filename)[0]
    if directory:
        os.makedirs(directory, exist_ok=True)
    con = sqlite3.connect(filename)
    cur = con.cursor()
    with open('output.sql') as f:
        cur.executescript(f.read())
    cur.close()
    con.commit()
    return con


def save_annotations(con: sqlite3.Connection, bdrmapit: Bdrmapit, rupdates, iupdates, increment=100000, chunksize=10000):
    query = 'INSERT INTO annotation (addr, router, asn, conn_asn, org, conn_org, iasn, iorg, utype, itype, rtype) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    values = []
    pb = Progress(len(bdrmapit.graph.routers), 'Collecting annotations', increment=increment)
    for router in pb.iterator(bdrmapit.graph.routers):
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
            values.append(d)
            if len(values) == chunksize:
                cur = con.cursor()
                cur.executemany(query, values)
                cur.close()
                con.commit()
                values = []
    if values:
        cur = con.cursor()
        cur.executemany(query, values)
        cur.close()
        con.commit()


def save_aslinks(con: sqlite3.Connection, bdrmapit: Bdrmapit, rupdates, increment=100000, chunksize=10000):
    query = 'INSERT INTO aslinks (addr, router, asn, conn_asns) VALUES (?, ?, ?, ?)'
    values = []
    pb = Progress(len(bdrmapit.graph.routers_succ), 'Collecting aslinks', increment=increment)
    for router in pb.iterator(bdrmapit.graph.routers_succ):
        rasn = rupdates[router][0]
        conn_asns = defaultdict(set)
        alledges = [
            ('next', bdrmapit.graph.rnexthop[router]), ('echo', bdrmapit.graph.recho[router]),
            ('multi', bdrmapit.graph.rmulti[router])
        ]
        for etype, edges in alledges:
            for isucc in edges:
                rsucc = bdrmapit.graph.interface_router[isucc]
                casn = rupdates[rsucc][0]
                if casn < 0:
                    casn = bdrmapit.lhupdates[rsucc][0]
                if casn > 0 and casn != rasn:
                # if casn > 0:
                    conn_asns[etype].add(casn)
        if not conn_asns:
            continue
        conn_asns = {k: list(v) for k, v in conn_asns.items()}
        conn_asns_json = json.dumps(conn_asns)
        for interface in bdrmapit.graph.router_interfaces[router]:
            addr = interface.address
            values.append((addr, router.name, rasn, conn_asns_json))
            if len(values) == chunksize:
                cur = con.cursor()
                cur.executemany(query, values)
                cur.close()
                con.commit()
                values = []
    if values:
        cur = con.cursor()
        cur.executemany(query, values)
        cur.close()
        con.commit()


def run(row: Experiment):
    bgp = BGP(row.rels, row.cone)
    as2org = AS2Org(row.as2org, include_potaroo=False)
    ip2as = RoutingTable.ip2as(row.ip2as)
    graph = HybridGraph()
    co = CreateObjs(row.db, graph, ip2as, as2org, bgp)
    co.read_addresses()
    if row.nodes:
        co.alias_resolution(row.nodes)
    else:
        graph.router_interfaces.finalize()
    co.create_graph()
    co.destpairs()
    graph.set_routers_interfaces()
    bdrmapit = Bdrmapit(graph, as2org, bgp, step=0)
    lh.annotate_lasthops(bdrmapit, routers=graph.routers_nosucc)
    rupdates, iupdates = alg.graph_refinement(bdrmapit, graph.routers_succ, graph.interfaces_pred, iterations=row.iterations)
    con = opendb(row.output, remove=True)
    save_annotations(con, bdrmapit, rupdates, iupdates)
    save_aslinks(con, bdrmapit, rupdates)
    con.close()


def run_config(filename, processes, iterations, sheet=None, no_clobber=None):
    try:
        df = pd.read_csv(filename)
    except:
        df = pd.read_excel(filename, sheet_name=(sheet if sheet is not None else 0))
    df['iterations'] = df.iterations.fillna(iterations) if 'iterations' in df else iterations
    rows = [Experiment(**{k: v for k, v in row._asdict().items() if k in Experiment._fields}) for row in df.itertuples()]
    if no_clobber:
        rows = [r for r in rows if not os.path.exists(r.output)]
    pb = Progress(len(rows), 'Running bdrmapits', force=True)
    if len(rows) > 1 or processes != 1:
        Progress.set_output(False)
        with Pool(min(processes, len(rows))) as pool:
            for _ in pb.iterator(pool.imap_unordered(run, rows)):
                pass
    else:
        for row in pb.iterator(rows):
            run(row)


def main():
    parser = ArgumentParser()
    parser.add_argument('-d', '--db', help='DB with output from parser.')
    parser.add_argument('-n', '--nodes', help='The ITDK nodes file.')
    parser.add_argument('-i', '--ip2as', help='BGP prefix file regex to use.')
    parser.add_argument('-A', '--as2org', help='AS-to-Org mappings in the standard CAIDA format.')
    parser.add_argument('-r', '--rels', help='AS relationship file in the standard CAIDA format.')
    parser.add_argument('-c', '--cone', help='AS customer cone file in the standard CAIDA format.')
    parser.add_argument('-I', '--iterations', default=-1, type=int,
                        help='Maximum number of iterations of the graph refinement loop.')
    parser.add_argument('--config', help='Use config file instead of command line.')
    parser.add_argument('--sheet', help='When using an Excel file, you can specify the sheet.')
    parser.add_argument('--processes', default=1, type=int)
    parser.add_argument('--no-clobber', action='store_true')
    parser.add_argument('-o', '--output', help='Sqlite database for output.')
    args = parser.parse_args()

    if args.config:
        run_config(args.config, args.processes, args.iterations, args.sheet, args.no_clobber)
    else:
        row = Experiment(**{k: v for k, v in vars(args).items() if k in Experiment._fields})
        run(row)


if __name__ == '__main__':
    main()
