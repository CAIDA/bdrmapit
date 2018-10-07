from math import floor
from typing import List, Dict, Set, Any

from as2org import AS2Org
from bgp.bgp import BGP
from graph.abstract_graph import InterfaceRouter, DictBackup, RouterInterfaces, RouterDests, AbstractGraph
from graph.interface import Interface
from graph.router import Router
from utils.progress import Progress
from utils.utils import DictSet


class PriorityDict:
    def __init__(self):
        self.priority: Dict[Any, float] = {}
        self.data: DictSet[Any, Set[Any]] = DictSet()
        self.data.finalize()

    def __contains__(self, item):
        return item in self.data

    def __getitem__(self, item):
        return self.data[item]

    def __iter__(self):
        yield from self.data

    def add(self, k, v, priority):
        current = self.priority.get(k, float('inf'))
        if priority == current:
            self.data[k].add(v)
        elif priority < current:
            self.priority[k] = priority
            self.data[k] = {v}

    def get(self, k, default=None):
        return self.data.get(k, default=default)


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
        self.redges = PriorityDict()
        self.iedges = PriorityDict()
        self.rases = PriorityDict()
        self.rinterfaces = PriorityDict()
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
            multiples = floor(icmp_type / 256)
            remainder = icmp_type % 256
            if distance == 1:
                if remainder != 0:
                    priority = 1
                else:
                    priority = 2
            else:
                priority = 3
            if multiples > 0:
                priority += .5
            if multiples == 1:
                self.redges.add(yrouter, x, priority)
                self.rases.add((yrouter, x), y.asn, priority)
                self.rinterfaces.add((yrouter, x), y, priority)
            elif multiples == 2:
                self.redges.add(xrouter, y, priority)
                self.rases.add((xrouter, y), x.asn, priority)
                self.rinterfaces.add((xrouter, y), x, priority)
            else:
                self.redges.add(xrouter, y, priority)
                self.rases.add((xrouter, y), x.asn, priority)
                self.rinterfaces.add((xrouter, y), x, priority)
                self.iedges.add(y, x, priority)
            return 1
        return 0

    def finalize_dests(self):
        self.interface_dests.finalize()

    def finalize_edges(self):
        pass

    def finalize_routers(self):
        self.router_interfaces.finalize()

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
            if interface in self.iedges:
                self.interfaces_pred.append(interface)
            router = self.interface_router[interface]
            if router != interface:
                if router in routers:
                    continue
                routers.add(router)
            self.routers.append(router)
            if router in self.redges:
                self.routers_succ.append(router)
            else:
                self.routers_nosucc.append(router)
