import os
from argparse import ArgumentParser
from multiprocessing.pool import Pool

from sqlalchemy import MetaData, Table, Column, Text, Integer
from sqlalchemy.engine import Engine, create_engine

import algorithm_cy as alg
import create_objs_cy as co
import last_hop as lh
from as2org import AS2Org
from bgp.bgp import BGP
from bgp.routing_table import RoutingTable
from graph.bdrmapit import Bdrmapit
from graph.hybrid_graph import HybridGraph
from utils.progress import Progress
import pandas as pd
from collections import namedtuple


Experiment = namedtuple('Experiment', ['addrs', 'nodes', 'adjs', 'dps', 'dists', 'output', 'ip2as', 'as2org', 'rels', 'cone', 'iterations'])


def opendb(filename, remove=False):
    if remove:
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
    engine = create_engine('sqlite:///{}'.format(filename))
    meta = MetaData()
    annotation = Table(
        'annotation', meta,
        Column('addr', Text),
        Column('router', Text),
        Column('asn', Integer),
        Column('conn_asn', Integer),
        Column('org', Text),
        Column('conn_org', Text),
        Column('iasn', Text),
        Column('iorg', Text),
        Column('rtype', Integer)
    )
    meta.create_all(engine)
    return engine, meta


def save(engine: Engine, meta: MetaData, bdrmapit: Bdrmapit, rupdates, iupdates, increment=100000, chunksize=10000):
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
        rasn, rorg, _ = bdrmapit.lhupdates.get(router, rupdates[router])
        for interface in bdrmapit.graph.router_interfaces[router]:
            if interface.org == rorg or interface.asn == 0:
                iasn, iorg, _ = iupdates[interface]
                if iasn == -1:
                    iasn, iorg, _ = interface.asn, interface.org, -1
            else:
                iasn, iorg, _ = interface.asn, interface.org, -2
            d = dict(addr=interface.address, router=router.name, conn_asn=iasn, conn_org=iorg, asn=rasn, org=rorg,
                     iasn=interface.asn, iorg=interface.org, rtype=rtype)
            values.append(d)
            if len(values) == chunksize:
                engine.execute(meta.tables['annotation'].insert(), values)
                values = []
    if values:
        engine.execute(meta.tables['annotation'].insert(), values)


def run(row: Experiment):
    bgp = BGP(row.rels, row.cone)
    as2org = AS2Org(row.as2org, include_potaroo=False)
    ip2as = RoutingTable.ip2as(row.ip2as)
    graph = HybridGraph()
    co.read_addresses(row.addrs, graph, ip2as, as2org)
    if row.nodes:
        co.alias_resolution(row.nodes, graph)
    else:
        graph.router_interfaces.finalize()
    true_adjs = co.adjacencies(row.dists)
    co.create_graph(row.adjs, graph, true_adjs)
    co.destpairs(row.dps, graph, as2org, bgp)
    graph.set_routers_interfaces()
    bdrmapit = Bdrmapit(graph, as2org, bgp, step=0)
    echo_routers = [router for router in graph.routers_succ if alg.get_edges(bdrmapit, router)[1] == 2]
    noecho_routers = [r for r in graph.routers_succ if r in graph.rnexthop or r in graph.rmulti]
    all_lasthop_routers = graph.routers_nosucc + echo_routers
    lh.annotate_lasthops(bdrmapit, routers=all_lasthop_routers)
    rupdates, iupdates = alg.graph_refinement(bdrmapit, noecho_routers, graph.interfaces_pred,
                                              iterations=row.iterations)
    engine, meta = opendb(row.output, remove=True)
    save(engine, meta, bdrmapit, rupdates, iupdates)


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
    parser.add_argument('-a', '--adjs', help='Adjacency output file.')
    parser.add_argument('-b', '--addrs', help='Addresses seen in the traceroutes.')
    parser.add_argument('-n', '--nodes', help='The ITDK nodes file.')
    parser.add_argument('-d', '--dps', help='Dest pairs output file.')
    parser.add_argument('-e', '--dists', help='Distances between interfaces.')
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
