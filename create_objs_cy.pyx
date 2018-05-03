from distutils.util import strtobool
from typing import Tuple, Set

from as2org cimport AS2Org
from bgp.bgp cimport BGP
from bgp.routing_table import RoutingTable
from graph.hybrid_graph cimport HybridGraph
from graph.router cimport Router
from utils.progress import Progress
from utils.utils import File2

def read_addresses(str filename, HybridGraph g, ip2as: RoutingTable, AS2Org as2org, int increment=100000):
    cdef str address, org, line
    cdef long asn
    cdef int i = 0
    pb = Progress(message='Addresses', increment=increment, callback=lambda: '{:,d}'.format(i))
    with open(filename) as f:
        f.readline()
        for line in pb.iterator(f):
            address = line[:-1]
            asn = ip2as[address]
            if asn > -2:
                org = as2org[asn]
                g.add_interface(address, asn, org)
                i += 1
    return g

def alias_resolution(str filename, HybridGraph g, int increment=1000000):
    cdef str line, n, address, nid
    cdef list addresses
    cdef Router router
    cdef int i = 0
    pb = Progress(message='Reading nodes', increment=increment, callback=lambda: 'Routers {:,d}'.format(i))
    with File2(filename) as f:
        for line in pb.iterator(f):
            if line[0] != '#':
                _, n, *addresses = line.split()
                addresses = [address for address in addresses if address in g.address_interface]
                if len(addresses) > 1:
                    nid = n[:-1]
                    router = g.add_router(nid)
                    g.group_interfaces(router, addresses)
                    i += 1
    g.router_interfaces.finalize()

def adjacencies(str filename, int increment=1000000) -> Set[Tuple[str, str]]:
    cdef set adjs = set()
    cdef str line, h1, h2, dist_s
    cdef int i = 0
    cdef long dist
    pb = Progress(message='Reading dists', increment=increment)
    with File2(filename) as f:
        f.readline()
        for line in pb.iterator(f):
            h1, h2, dist_s = line.split(',')
            adjs.add((h1, h2))
    return adjs

def create_graph(str filename, HybridGraph g, set adjacent, int increment=1000000):
    cdef str line, h1, h2
    cdef int dist, icmp_type
    cdef int used = 0
    cdef int modified = 0
    pb = Progress(message='Adding neighbors', increment=increment, callback=lambda: 'Used {:,d} Modified {:,d}'.format(used, modified))
    with File2(filename) as f:
        f.readline()
        for line in pb.iterator(f):
            splits = line.rstrip().split(',')
            h1 = splits[0]
            h2 = splits[1]
            dist = int(splits[2])
            if dist > 0:
                icmp_type = int(splits[3])
                if dist == 1 and (h1, h2) not in adjacent:
                    dist = 10
                    modified += 1
                g.add_edge(h1, h2, dist, icmp_type)
                used += 1
    g.rnexthop.finalize()
    g.recho.finalize()
    g.rmulti.finalize()
    g.rnh_ases.finalize()
    g.re_ases.finalize()
    g.rm_ases.finalize()
    g.inexthop.finalize()

def destpairs(str filename, HybridGraph g, AS2Org as2org, BGP bgp, increment=500000):
    cdef int used = 0
    cdef long dest_asn
    cdef str address
    cdef bint echo
    pb = Progress(message='Dest pairs', increment=increment, callback=lambda: 'Used {:,d}'.format(used))
    with File2(filename) as f:
        f.readline()
        for line in pb.iterator(f):
            splits = line.rstrip().split(',')
            address = splits[0]
            dest_asn = int(splits[1])
            echo = strtobool(splits[2])
            if dest_asn > 0 and not echo:
                g.add_dest(address, dest_asn)
                used += 1
    g.interface_dests.finalize()
    g.set_dests(as2org, bgp)
