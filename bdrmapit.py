from argparse import ArgumentParser, FileType
from logging import getLogger, INFO
from sys import stdout

import pandas as pd

import algorithm_steps as alg
import bgp.routing_table as rt
import create_objs_container as coi
from as2org import AS2Org
from bgp.bgp import BGP
from traceroute.paths import Paths
from updates_dict import Updates

log = getLogger()
log.setLevel(INFO)


def external_data(year, month, day, prefixes):
    log.info('Creating BGP, AS2Org, and IP2AS')
    bgp = BGP(year=year, month=month)
    as2org = AS2Org(year=year, month=month, include_potaroo=False)
    ip2as = rt.default_routing_table(prefixes, as2org=as2org, bgp=bgp, rir=True, year=year, month=month, day=day)
    return bgp, as2org, ip2as


def main():
    parser = ArgumentParser()
    parser.add_argument('-a', '--adj', required=True, help='Adjacency output file.')
    parser.add_argument('-n', '--nodes', help='The ITDK nodes file.')
    parser.add_argument('-d', '--dp', required=True, help='Dest pairs output file.')
    parser.add_argument('-e', '--dist', required=True, help='Distances between interfaces.')
    parser.add_argument('-i', '--ip2as', required=True, help='BGP prefix file regex to use.')
    parser.add_argument('-A', '--as2org', help='AS-to-Org mappings in the standard CAIDA format.')
    parser.add_argument('-r', '--rels', help='AS relationship file in the standard CAIDA format.')
    parser.add_argument('-c', '--cone', help='AS customer cone file in the standard CAIDA format.')
    parser.add_argument('-o', '--output', default=stdout, type=FileType('w'), help='File where the output files will be written.')
    args = parser.parse_args()

    bgp = BGP(args.rels, args.cone)
    as2org = AS2Org(args.as2org, include_potaroo=False)
    ip2as = rt.RoutingTable.ip2as(args.ip2as)
    log.info('Creating the graph')
    create = coi.NodesContainer()
    create.create_nodes(args.nodes, ip2as, as2org, increment=100000)
    adjacent = {(h1, h2) for chunk in pd.read_csv(args.dist, dtype={'Hop1': str, 'Hop2': str, 'Distance': int}, na_filter=False, chunksize=100000) for h1, h2, d in chunk.itertuples(index=False, name=None) if d >= 0}
    create.identify_neighbors(args.adj, adjacent, chunksize=100000, increment=500000, ip2as=ip2as, as2org=as2org)
    alg.annotate_initial(create, bgp=bgp)
    paths = Paths(args.dp, create, as2org, loop=True)
    lh = alg.gateway_heuristic(create, Updates(), paths=paths, bgp=bgp, as2org=as2org, utype=0)
    gr = alg.algorithm(create, updates=lh, bgp=bgp, paths=paths, utype=1, iterations=1, check_done=True, as2org=as2org)
    log.info('Writing results')
    gr.results(create.interfaces.values(), updates_only=False, networks=None, verbose=False).to_csv(args.output)
    log.info('Done. Cleaning up.')


if __name__ == '__main__':
    main()
