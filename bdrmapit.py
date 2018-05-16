#!/usr/bin/env python
import os
import sqlite3
from argparse import ArgumentParser
from multiprocessing.pool import Pool

import algorithm_restart as alg
from create_objs_sqlite import CreateObjs
import last_hop as lh
from as2org import AS2Org
from bgp.bgp import BGP
from bgp.routing_table import RoutingTable
from graph.bdrmapit import Bdrmapit
from graph.hybrid_graph import HybridGraph
from utils.progress import Progress
import pandas as pd
from collections import namedtuple


Experiment = namedtuple('Experiment', ['db', 'nodes', 'output', 'ip2as', 'as2org', 'rels', 'cone', 'iterations'])


def opendb(filename, remove=False):
    if remove:
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
    con = sqlite3.connect(filename)
    cur = con.cursor()
    cur.execute('''CREATE TABLE IF NOT EXISTS annotation (
      addr TEXT,
      router TEXT,
      asn INT,
      conn_asn INT,
      org TEXT,
      conn_org TEXT,
      iasn INT,
      iorg TEXT,
      utype INT,
      itype INT,
      rtype INT
    )''')
    cur.close()
    con.commit()
    return con


def save(con: sqlite3.Connection, bdrmapit: Bdrmapit, rupdates, iupdates, increment=100000, chunksize=10000):
    values = []
    pb = Progress(len(bdrmapit.graph.routers), 'Collecting annotations', increment=increment)
    cur = con.cursor()
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
                cur.executemany('INSERT INTO annotation (addr, router, asn, conn_asn, org, conn_org, iasn, iorg, utype, itype, rtype) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', values)
                cur.close()
                con.commit()
                cur = con.cursor()
                values = []
    if values:
        cur.executemany('INSERT INTO annotation (addr, router, asn, conn_asn, org, conn_org, iasn, iorg, utype, itype, rtype) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)', values)
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
    # echo_routers = [router for router in graph.routers_succ if alg.get_edges(bdrmapit, router)[1] == 2]
    # noecho_routers = [r for r in graph.routers_succ if r in graph.rnexthop or r in graph.rmulti]
    # all_lasthop_routers = graph.routers_nosucc + echo_routers
    # lh.annotate_lasthops(bdrmapit, routers=all_lasthop_routers)
    # rupdates, iupdates = alg.graph_refinement(bdrmapit, noecho_routers, graph.interfaces_pred, iterations=row.iterations)
    lh.annotate_lasthops(bdrmapit, routers=graph.routers_nosucc)
    rupdates, iupdates = alg.graph_refinement(bdrmapit, graph.routers_succ, graph.interfaces_pred, iterations=row.iterations)
    con = opendb(row.output, remove=True)
    save(con, bdrmapit, rupdates, iupdates)


def run_config(filename, processes, iterations):
    try:
        df = pd.read_csv(filename)
    except:
        df = pd.read_excel(filename)
    rows = [Experiment(iterations=iterations, **{k: v for k, v in row._asdict().items() if k in Experiment._fields}) for row in df.itertuples()]
    pb = Progress(len(rows), 'Running bdrmapits', force=True)
    if len(df) > 1 or processes != 1:
        Progress.set_output(False)
        with Pool(processes) as pool:
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
    parser.add_argument('--processes', default=1, type=int)
    parser.add_argument('-o', '--output', help='Sqlite database for output.')
    args = parser.parse_args()

    if args.config:
        run_config(args.config, args.processes, args.iterations)
    else:
        row = Experiment(**{k: v for k, v in vars(args).items() if k in Experiment._fields})
        run(row)


if __name__ == '__main__':
    main()
