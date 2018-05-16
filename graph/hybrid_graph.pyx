import pickle
from typing import List, Iterator

from as2org cimport AS2Org
from bgp.bgp cimport BGP
from graph.interface cimport Interface
from graph.router cimport Router
from utils.progress import Progress
from utils.utils cimport DictList, DictSet

cdef public int NEXTHOP = 1
cdef public int ECHO = 2
cdef public int MULTI = 3

cdef class NameRouter(dict):
    def __init__(self, dict interface_dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interface_dict = interface_dict

    def __missing__(self, key):
        return self.interface_dict[key]

cdef class RouterInterfaces(DictList):
    def __missing__(self, key):
        if self.finalized:
            return [key]
        else:
            return super().__missing__(key)

cdef class RouterDests(DictSet):
    def __init__(self, DictSet interface_dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interface_dict = interface_dict

    def __missing__(self, key):
        if self.finalized:
            return self.interface_dict[key]
        else:
            return super().__missing__(key)

cdef class InterfaceRouter(dict):
    def __missing__(self, key):
        return key
        

cdef class HybridGraph:
    @classmethod
    def from_graph(cls, o):
        g = cls()
        g.__dict__.update(o.__dict__.copy())
        return g

    @classmethod
    def open(cls, filename, reload=False):
        with open(filename, 'rb') as f:
            p = pickle.load(f)
        if reload:
            return p.copy()
        return p

    def __init__(self):
        self.address_interface = {}
        self.interface_router = InterfaceRouter()
        self.name_router = NameRouter(self.address_interface)
        self.router_interfaces = RouterInterfaces()
        self.interface_dests = DictSet()
        self.router_dests = RouterDests(self.interface_dests)
        self.modified_interface_dests = DictSet()
        self.modified_router_dests = RouterDests(self.modified_interface_dests)
        self.rnexthop = DictSet()
        self.recho = DictSet()
        self.rmulti = DictSet()
        self.inexthop = DictSet()
        self.interface_succtype = {}
        self.rnh_ases = DictSet()
        self.re_ases = DictSet()
        self.rm_ases = DictSet()
        self.rnh_interfaces = DictSet()
        self.re_interfaces = DictSet()
        self.rm_interfaces = DictSet()
        self.routers = []
        self.routers_succ = []
        self.routers_nosucc = []
        self.interfaces_pred = []

    cdef void add_interface(self, str address, int asn, str org) except *:
        cdef Interface interface = Interface(address, asn, org)
        self.address_interface[address] = interface

    cdef Router add_router(self, str name):
        cdef Router router = Router(name)
        self.name_router[name] = router
        return router

    cdef void add_dest(self, str address, int asn) except *:
        cdef Interface interface = self.address_interface[address]
        self.interface_dests[interface].add(asn)

    cdef int add_edge(self, str xaddr, str yaddr, int distance, int icmp_type) except -1:
        cdef Interface x = self.address_interface[xaddr]
        cdef Interface y = self.address_interface[yaddr]
        cdef Router xrouter = self.interface_router[x]
        cdef Router yrouter = self.interface_router[y]
        cdef int succtype
        if xrouter != yrouter:
            if x.asn == y.asn or distance == 1:
                if icmp_type != 0:
                    self.rnexthop[xrouter].add(y)
                    self.rnh_ases[xrouter, y].add(x.asn)
                    self.rnh_interfaces[xrouter, y].add(x)
                    self.inexthop[y].add(x)
                else:
                    self.recho[xrouter].add(y)
                    self.re_ases[xrouter, y].add(x.asn)
                    self.re_interfaces[xrouter, y].add(x)
            else:
                self.rmulti[xrouter].add(y)
                self.rm_ases[xrouter, y].add(x.asn)
                self.rm_interfaces[xrouter, y].add(x)
            return 1
        return 0

    cpdef HybridGraph copy(self):
        return HybridGraph.from_graph(self)

    cpdef set iedges(self, Interface interface):
        return self.inexthop[interface]

    cpdef set redges(self, Router router, int priority):
        if priority == NEXTHOP:
            return self.rnexthop.get(router)
        elif priority == ECHO:
            return self.recho.get(router)
        elif priority == MULTI or priority == MULTIECHO:
            return self.rmulti.get(router)
        else:
            raise ValueError('Invalid priority: {}'.format(priority))
    
    cpdef set origin_ases(self, Router router, Interface interface, int priority):
        if priority == NEXTHOP:
            return self.rnh_ases[router, interface]
        elif priority == ECHO:
            return self.re_ases[router, interface]
        elif priority == MULTI or priority == MULTIECHO:
            return self.rm_ases[router, interface]
        else:
            raise ValueError('Invalid priority: {}'.format(priority))

    cpdef set router_edge_dests(self, Router router, Interface subsequent, int rtype = 0):
        cdef DictSet d
        cdef set dests = set()
        cdef Interface interface
        if rtype == 0:
            if router in self.rnexthop:
                rtype = NEXTHOP
            elif router in self.recho:
                rtype = ECHO
            else:
                rtype = MULTI
        if rtype == NEXTHOP:
            d = self.rnh_interfaces
        elif rtype == ECHO:
            d = self.re_interfaces
        else:
            d = self.rm_interfaces
        for interface in d[router, subsequent]:
            dests.update(self.modified_interface_dests[interface])
        return dests

    def routers_degree(self) -> Iterator[Router]:
        for router in self.name_router.values():
            yield router, len(self.rnexthop.get(router, [])) + len(self.recho.get(router, [])) + len(
                self.rmulti.get(router, []))

    def interfaces_degree(self) -> Iterator[Router]:
        for interface in self.address_interface.values():
            yield interface, len(self.inexthop.get(interface, []))

    def filter_addresses(self, addresses: List[str]):
        for address in addresses:
            if address in self.address_interface:
                yield address

    cpdef void group_interfaces(self, router, interfaces) except *:
        cdef list rlist
        if isinstance(router, str):
            router = self.name_router[router]
        rlist = self.router_interfaces[router]
        for interface in interfaces:
            if isinstance(interface, str):
                interface = self.address_interface[interface]
            rlist.append(interface)
            self.interface_router[interface] = router

    cpdef Interface interface(self, str address):
        return self.address_interface[address]

    cpdef Router router(self, str name):
        return self.name_router[name]

    cpdef int num_interfaces(self) except -1:
        return len(self.address_interface)

    cpdef int num_routers(self) except -1:
        return len(self.name_router)

    cpdef void save(self, str filename) except *:
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    def set_dests(self, AS2Org as2org, BGP bgp, int increment=100000):
        cdef Interface interface
        cdef Router router
        cdef set idests, rdests, rmodified
        cdef list interfaces
        self.router_dests.unfinalize()
        self.modified_router_dests.unfinalize()
        self.modified_interface_dests.unfinalize()
        pb = Progress(len(self.interface_dests), 'Modifying interface dests', increment=increment)
        for interface, idests in pb.iterator(self.interface_dests.items()):
            idests = self.interface_dests[interface]
            if idests:
                orgs = {as2org[a] for a in idests}
                if len(orgs) == 2 and interface.asn in idests:
                    if max(idests, key=lambda x: (bgp.conesize[x], -x)) == interface.asn:
                        idests = idests - {interface.asn}
                self.modified_interface_dests[interface].update(idests)
        pb = Progress(len(self.router_interfaces), 'Setting destinations', increment=increment)
        for router, interfaces in pb.iterator(self.router_interfaces.items()):
            rdests = self.router_dests[router]
            rmodified = self.modified_router_dests[router]
            for interface in interfaces:
                rdests.update(self.interface_dests[interface])
                rmodified.update(self.modified_interface_dests[interface])
        self.router_dests.finalize()
        self.modified_router_dests.finalize()
        self.modified_interface_dests.finalize()
                            

    def set_routers_interfaces(self, int increment=500000):
        cdef Interface interface
        cdef Router router
        cdef int degree
        cdef set routers = set()
        pb = Progress(len(self.address_interface), 'Routers and interfaces', increment=increment,
                      callback=lambda: 'Succ {:,d} NoSucc {:,d} Pred {:,d}'.format(len(self.routers_succ), len(self.routers_nosucc), len(self.interfaces_pred)))
        for interface in pb.iterator(self.address_interface.values()):
            if interface in self.inexthop:
                self.interfaces_pred.append(interface)
            router = self.interface_router[interface]
            if router != interface:
                if router in routers:
                    continue
                routers.add(router)
            self.routers.append(router)
            if router in self.rnexthop or router in self.recho or router in self.rmulti:
                self.routers_succ.append(router)
            else:
                self.routers_nosucc.append(router)