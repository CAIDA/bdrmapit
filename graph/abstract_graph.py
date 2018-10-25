import pickle
from abc import ABC, abstractmethod
from typing import List, Iterator, Dict, Set, Tuple, Any

from as2org import AS2Org
from bgp.bgp import BGP
from graph.interface import Interface
from graph.router import Router
from utils.utils import DictList, DictSet

NEXTHOP = 1
ECHO = 2
MULTI = 3


class DictBackup(dict):
    def __init__(self, interface_dict: Dict, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interface_dict = interface_dict

    def __missing__(self, key):
        return self.interface_dict[key]


class RouterInterfaces(DictList):
    def __missing__(self, key):
        if self.finalized:
            return [key]
        else:
            return super().__missing__(key)


class RouterDests(DictSet):
    def __init__(self, interface_dict: Dict[Interface, Set[int]], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.interface_dict = interface_dict

    def __missing__(self, key):
        if self.finalized:
            return self.interface_dict[key]
        else:
            return super().__missing__(key)


class InterfaceRouter(dict):
    def __missing__(self, key):
        return key


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


class AbstractGraph(ABC):
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
        self.address_interface: Dict[str, Interface] = NotImplemented
        self.interface_router: Dict[Interface, Router] = NotImplemented
        self.name_router: Dict[str, Router] = NotImplemented
        self.router_interfaces: Dict[Router, List[Interface]] = NotImplemented
        self.interface_dests: Dict[Interface, Set[int]] = NotImplemented
        self.router_dests: Dict[Router, Set[int]] = NotImplemented
        self.modified_interface_dests: Dict[Interface, Set[int]] = NotImplemented
        self.modified_router_dests: Dict[Router, Set[int]] = NotImplemented
        self.redges: PriorityDict = NotImplemented
        self.iedges: PriorityDict = NotImplemented
        self.rases: PriorityDict = NotImplemented
        self.rinterfaces: PriorityDict = NotImplemented
        self.routers: List[Router] = NotImplemented
        self.routers_succ: List[Router] = NotImplemented
        self.routers_nosucc: List[Router] = NotImplemented
        self.interfaces_pred: List[Interface] = NotImplemented

    @abstractmethod
    def add_interface(self, address: str, asn: int, org: str, num: int):
        raise NotImplementedError()

    @abstractmethod
    def add_router(self, name: str):
        raise NotImplementedError()

    @abstractmethod
    def add_dest(self, address: str, asn: int):
        raise NotImplementedError()

    @abstractmethod
    def add_edge(self, xaddr: str, yaddr: str, distance: int, icmp_type: int, special: int) -> int:
        raise NotImplementedError()

    @abstractmethod
    def clear_edges(self):
        raise NotImplementedError()

    def copy(self):
        return self.from_graph(self)

    @abstractmethod
    def finalize_dests(self):
        raise NotImplementedError()

    @abstractmethod
    def finalize_edges(self):
        raise NotImplementedError()

    @abstractmethod
    def finalize_routers(self):
        raise NotImplementedError()

    def routers_degree(self) -> Iterator[Router]:
        for router in self.name_router.values():
            yield router, len(self.rnexthop.get(router, [])) + len(self.recho.get(router, [])) + len(
                self.rmulti.get(router, []))

    def interfaces_degree(self) -> Iterator[Router]:
        for interface in self.address_interface.values():
            yield interface, len(self.inexthop.get(interface, []))

    def group_interfaces(self, router, interfaces):
        raise NotImplementedError()

    def interface(self, address):
        return self.address_interface[address]

    def router(self, name):
        return self.name_router[name]

    def num_interfaces(self):
        return len(self.address_interface)

    def num_routers(self):
        return len(self.name_router)

    def save(self, filename):
        with open(filename, 'wb') as f:
            pickle.dump(self, f)

    @abstractmethod
    def set_dests(self, as2org: AS2Org, bgp: BGP, increment=100000):
        raise NotImplementedError()

    @abstractmethod
    def set_routers_interfaces(self, increment=500000):
        raise NotImplementedError()
