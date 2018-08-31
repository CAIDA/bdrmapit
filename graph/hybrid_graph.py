from typing import List, Dict, Set

from as2org import AS2Org
from bgp.bgp import BGP
from graph.abstract_graph import InterfaceRouter, DictBackup, RouterInterfaces, RouterDests, NEXTHOP, ECHO, MULTI, \
    AbstractGraph
from graph.interface import Interface
from graph.router import Router
from utils.progress import Progress
from utils.utils import DictSet


class HybridGraph(AbstractGraph):

    def __init__(self):
        super(HybridGraph, self).__init__()
        self.address_interface: Dict[str, Interface] = {}
        self.interface_router: Dict[Interface, Router] = InterfaceRouter()
        self.name_router: Dict[str, Router] = DictBackup(self.address_interface)
        self.router_interfaces: RouterInterfaces = RouterInterfaces()
        self.interface_dests = DictSet()
        self.router_dests = RouterDests(self.interface_dests)
        self.modified_interface_dests = DictSet()
        self.modified_router_dests = RouterDests(self.modified_interface_dests)
        self.rnexthop = DictSet()
        self.recho = DictSet()
        self.rmulti = DictSet()
        self.inexthop = DictSet()
        self.iecho = DictSet()
        self.imulti = DictSet()
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

    def add_interface(self, address: str, asn: int, org: str, num: int):
        interface = Interface(address, asn, org, num)
        self.address_interface[address] = interface

    def add_router(self, name: str):
        router = Router(name)
        self.name_router[name] = router
        return router

    def add_dest(self, address: str, asn: int):
        interface = self.address_interface[address]
        self.interface_dests[interface].add(asn)

    def add_edge(self, xaddr, yaddr, distance, icmp_type):
        x = self.address_interface[xaddr]
        y = self.address_interface[yaddr]
        xrouter = self.interface_router[x]
        yrouter = self.interface_router[y]
        if xrouter != yrouter:
            if distance == 1:
                if icmp_type != 0:
                    self.rnexthop[xrouter].add(y)
                    self.rnh_ases[xrouter, y].add(x.asn)
                    self.rnh_interfaces[xrouter, y].add(x)
                    self.inexthop[y].add(x)
                else:
                    self.recho[xrouter].add(y)
                    self.re_ases[xrouter, y].add(x.asn)
                    self.re_interfaces[xrouter, y].add(x)
                    self.iecho[y].add(x)
            else:
                self.rmulti[xrouter].add(y)
                self.rm_ases[xrouter, y].add(x.asn)
                self.rm_interfaces[xrouter, y].add(x)
                self.imulti[y].add(x)
            return 1
        return 0

    def finalize_dests(self):
        self.interface_dests.finalize()

    def finalize_edges(self):
        self.rnexthop.finalize()
        self.recho.finalize()
        self.rmulti.finalize()
        self.rnh_ases.finalize()
        self.re_ases.finalize()
        self.rm_ases.finalize()
        self.inexthop.finalize()

    def finalize_routers(self):
        self.router_interfaces.finalize()

    def previous_routers(self, interface):
        return {self.interface_router[p] for p in self.interfaces_pred}

    def iedges(self, interface):
        return self.inexthop[interface]

    def router_edge_dests(self, router, subsequent, rtype=0):
        dests = set()
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

    def filter_addresses(self, addresses: List[str]):
        for address in addresses:
            if address in self.address_interface:
                yield address

    def group_interfaces(self, router, interfaces):
        if isinstance(router, str):
            router = self.name_router[router]
        rlist = self.router_interfaces[router]
        for interface in interfaces:
            if isinstance(interface, str):
                interface = self.address_interface[interface]
            rlist.append(interface)
            self.interface_router[interface] = router

    def set_dests(self, as2org: AS2Org, bgp: BGP, increment=100000):
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

    def set_routers_interfaces(self, increment=500000):
        routers = set()
        pb = Progress(len(self.address_interface), 'Routers and interfaces', increment=increment,
                      callback=lambda: 'Succ {:,d} NoSucc {:,d} Pred {:,d}'.format(len(self.routers_succ),
                                                                                   len(self.routers_nosucc),
                                                                                   len(self.interfaces_pred)))
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
