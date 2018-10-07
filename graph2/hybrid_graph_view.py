from typing import Dict, Set

from as2org import AS2Org
from bgp.bgp import BGP
from graph.abstract_graph import AbstractGraph
from graph.hybrid_graph import HybridGraph, DictBackup
from graph.interface import Interface
from graph.router import Router
from utils.progress import Progress
from utils.utils import File2, DictSet


class NameRouterBackup(dict):
    def __init__(self, original, address_interface, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.original = original
        self.address_interface = address_interface

    def __missing__(self, key):
        if key in self.original:
            return self.original[key]
        return self.address_interface[key]


class HybridGraphView(AbstractGraph):

    def __init__(self, graph: AbstractGraph, copy=False):
        super().__init__()
        self.graph = graph
        self.address_interface = DictBackup(graph.address_interface) if not copy else dict(graph.address_interface)
        self.interface_router = DictBackup(graph.interface_router)
        self.name_router = NameRouterBackup(graph.name_router, self.address_interface)
        self.router_interfaces = DictBackup(graph.router_interfaces)
        self.interface_dests: Dict[Interface, Set[int]] = DictBackup(graph.interface_dests) if not copy else DictSet({k: set(v) for k, v in graph.interface_dests.items()})
        self.router_dests = DictBackup(graph.router_dests)
        self.modified_interface_dests = DictBackup(graph.modified_interface_dests)
        self.modified_router_dests = DictBackup(graph.modified_router_dests)
        self.rnexthop = DictBackup(graph.rnexthop)
        self.recho = DictBackup(graph.recho)
        self.rmulti = DictBackup(graph.rmulti)
        self.inexthop = DictBackup(graph.inexthop)
        self.iecho = DictBackup(graph.iecho)
        self.imulti = DictBackup(graph.imulti)
        self.interface_succtype = DictBackup(graph.interface_succtype)
        self.rnh_ases = DictBackup(graph.rnh_ases)
        self.re_ases = DictBackup(graph.re_ases)
        self.rm_ases = DictBackup(graph.rm_ases)
        self.rnh_interfaces = DictBackup(graph.rnh_interfaces)
        self.re_interfaces = DictBackup(graph.re_interfaces)
        self.rm_interfaces = DictBackup(graph.rm_interfaces)
        self.routers = []
        self.routers_succ = []
        self.routers_nosucc = []
        self.interfaces_pred = [] if not copy else list(graph.interfaces_pred)

    def add_interface(self, address: str, asn: int, org: str, num: int):
        if address in self.graph.address_interface and address not in self.address_interface:
            interface = self.graph.address_interface[address]
            router = self.graph.interface_router[interface]
            if router != interface:
                self.name_router[router.name] = router
        else:
            interface = Interface(address, asn, org, num)
        self.address_interface[address] = interface

    def add_router(self, name: str):
        router = Router(name)
        self.name_router[name] = router
        return router

    def add_dest(self, address: str, asn: int):
        interface = self.address_interface[address]
        if interface not in self.interface_dests:
            dests = self.graph.interface_dests[interface]
            if asn not in dests:
                dests = set(dests)
                dests.add(asn)
                self.interface_dests[interface] = dests
        else:
            self.interface_dests[interface].add(asn)

    def add_edge(self, xaddr, yaddr, distance, icmp_type):
        x = self.address_interface[xaddr]
        y = self.address_interface[yaddr]
        xrouter = self.interface_router[x]
        yrouter = self.interface_router[y]
        if xrouter != yrouter:
            if distance == 1:
                if icmp_type != 0:
                    nh = self.rnexthop
                    nh_ases = self.rnh_ases
                    nh_interfaces = self.rnh_interfaces
                    ph = self.inexthop
                    snh = self.graph.rnexthop
                    snh_ases = self.graph.rnh_ases
                    snh_interfaces = self.graph.rnh_interfaces
                    sph = self.graph.inexthop
                else:
                    nh = self.recho
                    nh_ases = self.re_ases
                    nh_interfaces = self.re_interfaces
                    ph = self.iecho
                    snh = self.graph.recho
                    snh_ases = self.graph.re_ases
                    snh_interfaces = self.graph.re_interfaces
                    sph = self.graph.iecho
            else:
                nh = self.rmulti
                nh_ases = self.rm_ases
                nh_interfaces = self.rm_interfaces
                ph = self.imulti
                snh = self.graph.rmulti
                snh_ases = self.graph.rm_ases
                snh_interfaces = self.graph.rm_interfaces
                sph = self.graph.imulti
            if xrouter not in nh:
                if y not in snh[xrouter]:
                    nh[xrouter] = set(snh[xrouter])
                    nh_ases[xrouter, y] = set(snh_ases[xrouter, y])
                    nh_interfaces[xrouter, y] = set(snh_interfaces[xrouter, y])
                    ph[y] = set(sph[x])
                else:
                    return 0
            nh[xrouter].add(y)
            nh_ases[xrouter, y].add(x.asn)
            nh_interfaces[xrouter, y].add(x)
            ph[y].add(x)
            return 1
        return 0

    def finalize_dests(self):
        pass

    def finalize_edges(self):
        pass

    def finalize_routers(self):
        pass

    def group_interfaces(self, router, interfaces):
        if isinstance(router, str):
            router = self.name_router[router]
        rnexthop = self.rnexthop.get(router, set())
        recho = self.recho.get(router, set())
        rmulti = self.rmulti.get(router, set())
        rlist = []
        for interface in interfaces:
            if isinstance(interface, str):
                interface = self.address_interface[interface]
            iasn = interface.asn
            rlist.append(interface)
            self.interface_router[interface] = router
            if interface.asn > -1:
                for succ in self.rnexthop[interface]:
                    if succ not in interfaces:
                        rnexthop.add(succ)
                        rnh_ases = self.rnh_ases.get((router, succ), set())
                        rnh_ases.add(iasn)
                        self.rnh_ases[router, succ] = rnh_ases
                        rnh_interfaces = self.rnh_interfaces.get((router, succ), set())
                        rnh_interfaces.add(succ)
                        self.rnh_interfaces[router, succ] = rnh_interfaces
                for succ in self.recho[interface]:
                    if succ not in interfaces:
                        recho.add(succ)
                        re_ases = self.re_ases.get((router, succ), set())
                        re_ases.add(iasn)
                        self.re_ases[router, succ] = re_ases
                        re_interfaces = self.re_interfaces.get((router, succ), set())
                        re_interfaces.add(succ)
                        self.re_interfaces[router, succ] = re_interfaces
                for succ in self.rmulti[interface]:
                    if succ not in interfaces:
                        rmulti.add(succ)
                        rm_ases = self.rm_ases.get((router, succ), set())
                        rm_ases.add(iasn)
                        self.rm_ases[router, succ] = rm_ases
                        rm_interfaces = self.rm_interfaces.get((router, succ), set())
                        rm_interfaces.add(succ)
                        self.rm_interfaces[router, succ] = rm_interfaces
        self.rnexthop[router] = rnexthop
        self.recho[router] = recho
        self.rmulti[router] = rmulti
        self.router_interfaces[router] = rlist

    def set_dests(self, as2org: AS2Org, bgp: BGP, increment=100000):
        pb = Progress(len(self.interface_dests), 'Modifying interface dests', increment=increment)
        for interface, idests in pb.iterator(self.interface_dests.items()):
            idests = self.interface_dests[interface]
            if idests:
                orgs = {as2org[a] for a in idests}
                if len(orgs) == 2 and interface.asn in idests:
                    if max(idests, key=lambda x: (bgp.conesize[x], -x)) == interface.asn:
                        idests = idests - {interface.asn}
                self.modified_interface_dests[interface] = idests
        pb = Progress(len(self.router_interfaces), 'Setting destinations', increment=increment)
        for router, interfaces in pb.iterator(self.router_interfaces.items()):
            rdests = set()
            rmodified = set()
            for interface in interfaces:
                rdests.update(self.interface_dests[interface])
                rmodified.update(self.modified_interface_dests[interface])
            self.router_dests[router] = rdests
            self.modified_router_dests[router] = rmodified

    def set_routers_interfaces(self, useall=False, increment=500000):
        routers = set()
        pb = Progress(len(self.address_interface), 'Routers and interfaces', increment=increment, callback=lambda: 'Succ {:,d} NoSucc {:,d} Pred {:,d}'.format(len(self.routers_succ), len(self.routers_nosucc), len(self.interfaces_pred)))
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
            elif router in self.graph.rnexthop or router in self.graph.recho or router in self.graph.rmulti:
                if useall:
                    self.routers_succ.append(router)
                continue
            else:
                self.routers_nosucc.append(router)
