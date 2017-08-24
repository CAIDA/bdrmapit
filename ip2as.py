import csv
from argparse import ArgumentParser, FileType

from as2org import AS2Org
from bgp.bgp import BGP
from bgp.routing_table import create_routing_table
from utils.utils import ls


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
    prefixes = list(ls(args.prefixes))
    rir = list(ls(args.rir))
    bgp = BGP(args.rels, args.cone)
    as2org = AS2Org(args.as2org, include_potaroo=False)
    ip2as = create_routing_table(prefixes, ixp_prefixes=args.ixp_prefixes, ixp_asns=None, rir=rir, bgp=bgp, as2org=as2org)
    nodes = ip2as.nodes()
    writer = csv.writer(args.output)
    writer.writerow(['prefix', 'asn'])
    writer.writerows([node.prefix, node.data['asn']] for node in nodes)


if __name__ == '__main__':
    main()
