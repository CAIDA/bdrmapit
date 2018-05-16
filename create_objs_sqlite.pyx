import sqlite3

from as2org cimport AS2Org
from bgp.bgp cimport BGP
from bgp.routing_table import RoutingTable
from graph.hybrid_graph cimport HybridGraph
from graph.router cimport Router
from utils.progress import Progress
from utils.utils import File2


cdef class CreateObjs:
    def __init__(self, str filename, HybridGraph g, ip2as: RoutingTable, AS2Org as2org, BGP bgp):
        self.con = sqlite3.connect(filename)
        self.g = g
        self.ip2as = ip2as
        self.as2org = as2org
        self.bgp = bgp

    def read_addresses(self, int increment=1000000):
        cdef int i = 0
        cur = self.con.cursor()
        pb = Progress(message='Addresses', increment=increment, callback=lambda: '{:,d}'.format(i))
        for addr, in pb.iterator(cur.execute('SELECT addr FROM address')):
            asn = self.ip2as[addr]
            if asn > -2:
                org = self.as2org[asn]
                self.g.add_interface(addr, asn, org)
                i += 1
        cur.close()

    def alias_resolution(self, str filename, int increment=1000000):
        cdef str line, n, address, nid
        cdef list addresses
        cdef Router router
        cdef int i = 0
        pb = Progress(message='Reading nodes', increment=increment, callback=lambda: 'Routers {:,d}'.format(i))
        with File2(filename) as f:
            for line in pb.iterator(f):
                if line[0] != '#':
                    _, n, *addresses = line.split()
                    addresses = [address for address in addresses if address in self.g.address_interface]
                    if len(addresses) > 1:
                        nid = n[:-1]
                        router = self.g.add_router(nid)
                        self.g.group_interfaces(router, addresses)
                        i += 1
        self.g.router_interfaces.finalize()

    def create_graph(self, int increment=1000000):
        cdef int used = 0, dist, icmp_type
        cdef set adjs
        cdef str h1, h2
        cur = self.con.cursor()
        print('Reading distances')
        adjs = set(cur.execute('select hop1, hop2 from distance where distance > 0'))
        pb = Progress(message='Adding neighbors', increment=increment, callback=lambda: 'Used {:,d}'.format(used))
        for h1, h2, dist, icmp_type in pb.iterator(cur.execute('select hop1, hop2, distance, type from adjacency where distance > 0')):
            if (h1, h2) not in adjs:
                dist = 10
            used += self.g.add_edge(h1, h2, dist, icmp_type)
        cur.close()
        self.g.rnexthop.finalize()
        self.g.recho.finalize()
        self.g.rmulti.finalize()
        self.g.rnh_ases.finalize()
        self.g.re_ases.finalize()
        self.g.rm_ases.finalize()
        self.g.inexthop.finalize()

    # def create_graph(self, int increment=1000000):
    #     cdef int used = 0
    #     cur = self.con.cursor()
    #     query = '''SELECT hop1, hop2, CASE WHEN d.distance > 0 THEN a.distance ELSE 10 END AS distance, a.type
    #     FROM adjacency a JOIN distance d USING (hop1, hop2)
    #     WHERE a.distance > 0'''
    #     pb = Progress(message='Adding neighbors', increment=increment, callback=lambda: 'Used {:,d}'.format(used))
    #     for h1, h2, dist, icmp_type in pb.iterator(cur.execute(query)):
    #         self.g.add_edge(h1, h2, dist, icmp_type)
    #         used += 1
    #     cur.close()
    #     self.g.rnexthop.finalize()
    #     self.g.recho.finalize()
    #     self.g.rmulti.finalize()
    #     self.g.rnh_ases.finalize()
    #     self.g.re_ases.finalize()
    #     self.g.rm_ases.finalize()
    #     self.g.inexthop.finalize()

    def destpairs(self, increment=500000):
        cdef int used = 0
        cdef long dest_asn
        cdef str address
        cdef bint echo
        cur = self.con.cursor()
        pb = Progress(message='Dest pairs', increment=increment, callback=lambda: 'Used {:,d}'.format(used))
        query = '''SELECT addr, asn FROM destpair WHERE asn > 0 AND not echo'''
        for addr, dest_asn in pb.iterator(cur.execute(query)):
            self.g.add_dest(addr, dest_asn)
            used += 1
        cur.close()
        self.g.interface_dests.finalize()
        self.g.set_dests(self.as2org, self.bgp)
