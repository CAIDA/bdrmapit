from typing import List, Dict

from as2org import AS2Org
from bgp.bgp import BGP
from graph.abstract_graph import InterfaceRouter, DictBackup, RouterInterfaces, RouterDests, AbstractGraph, PriorityDict
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
        self.redges = PriorityDict()
        self.iedges = PriorityDict()
        self.rases = PriorityDict()
        self.routers = []
        self.routers_succ = []
        self.routers_nosucc = []
        self.interfaces_pred = []
        self.rrrelated = DictSet()
        self.irrelated = DictSet()
        self.rirelated = DictSet()
        self.iirelated = DictSet()

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

    def add_edge(self, xaddr, yaddr, distance, icmp_type, special=0):
        x = self.address_interface[xaddr]
        y = self.address_interface[yaddr]
        xrouter = self.interface_router[x]
        yrouter = self.interface_router[y]
        if xrouter != yrouter:
            if distance == 1 or x.asn == y.asn:
                if icmp_type != 0:
                    priority = 1
                else:
                    priority = 2
            else:
                priority = 3
            if special == 1:
                priority += 0.1
                self.redges.add(xrouter, y, priority)
                self.redges.add(yrouter, x, priority)
                self.rases.add((xrouter, y), x.asn, priority)
                self.rases.add((yrouter, x), x.asn, priority)
            elif special == 2 or special == 3:
                priority += 0.1
                self.iedges.add(y, x, priority)
                self.iedges.add(x, y, priority)
            else:
                self.redges.add(xrouter, y, priority)
                self.rases.add((xrouter, y), x.asn, priority)
                self.iedges.add(y, x, priority)
            self.rrrelated[xrouter].add(yrouter)
            self.rrrelated[yrouter].add(xrouter)
            self.rirelated[xrouter].add(y)
            self.rirelated[yrouter].add(x)
            self.irrelated[x].add(yrouter)
            self.irrelated[y].add(xrouter)
            self.iirelated[x].add(y)
            self.iirelated[y].add(x)
            return 1
        return 0

    def clear_edges(self):
        self.redges = PriorityDict()
        self.iedges = PriorityDict()
        self.rases = PriorityDict()

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
            if self.iedges.priority.get(interface, float('inf')) < 2:
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
