import socket
from collections import defaultdict, Counter
from itertools import chain

import pandas as pd
import struct

from edge import Priority, Type
from interface import Interface
# from router import Router
from router_collapse import Router
from utils.progress import Progress, status, finish_status
from utils.utils import File2


class NodesContainerDict:
    def __init__(self, *names):
        self.containers = {name: NodesContainer() for name in names}

    def create_nodes(self, ip2as, as2org, increment=100000, **kargs):
        for name, filename in kargs.items():
            self.containers[name].create_nodes(filename, ip2as, as2org, increment=increment)

    def create_nodes_from_hops(self, ip2as, as2org, increment=1000000, check=False, **kargs):
        for name, filename in kargs.items():
            self.containers[name].create_nodes_from_hops(filename, ip2as, as2org, increment=increment, check=check)

    def identify_neighbors(self, increment=500000, chunksize=100, **kargs):
        for name, filename in kargs.items():
            self.containers[name].identify_neighbors(filename, increment=increment, chunksize=chunksize)

    def identify_neighbors_echo(self, increment=500000, chunksize=100, **kargs):
        for name, filename in kargs.items():
            self.containers[name].identify_neighbors_echo(filename, increment=increment, chunksize=chunksize)

    def identify_neighbors_private(self, increment=500000, chunksize=100, **kargs):
        for name, filename in kargs.items():
            self.containers[name].identify_neighbors_private(filename, increment=increment, chunksize=chunksize)

    def identify_neighbors_loop(self, updates, increment=500000, chunksize=100, **kargs):
        for name, filename in kargs.items():
            self.containers[name].identify_neighbors_loop(filename, updates, increment=increment, chunksize=chunksize)

    def othersides(self, *names, additional_addresses=None):
        for name in names:
            self.containers[name].othersides(additional_addresses=additional_addresses)

    @property
    def num_routers(self):
        return sum(len(self.containers[name].routers) for name in self.containers)

    @property
    def num_interfaces(self):
        return sum(len(self.containers[name].interfaces) for name in self.containers)

    @property
    def routers(self):
        for name in self.containers:
            yield from self.containers[name].routers.values()

    @property
    def interfaces(self):
        for name in self.containers:
            yield from self.containers[name].interfaces.values()


class NodesContainer:
    def __init__(self):
        self.routers = {}
        self.interfaces = {}

    def create_nodes(self, filename, ip2as, as2org, increment=100000):
        with File2(filename) as f:
            pb = Progress(message='Reading nodes', increment=increment, callback=lambda: 'Routers {:,d} Interfaces {:,d}'.format(len(self.routers), len(self.interfaces)))
            for line in pb.iterator(filter(lambda x: x[0] != '#', f)):
                _, n, *addresses = line.split()
                nid = n[:-1]
                self.create_objs(ip2as, as2org, nid, addresses)

    def create_objs(self, ip2as, as2org, nid, addresses):
        router = None
        for address in addresses:
            asn = ip2as[address]
            if asn > -2:
                if router is None:
                    router = Router(nid)
                    self.routers[nid] = router
                org = as2org[asn]
                interface = Interface(address, asn, org, router, add_to_router=True)
                self.interfaces[address] = interface

    def create_nodes_from_hops(self, filename, ip2as, as2org, increment=1000000, check=False):
        rid = len(self.routers) + 1
        # print(rid)
        with File2(filename) as f:
            pb = Progress(message='Reading nodes', increment=increment,callback=lambda: 'Routers {:,d} Interfaces {:,d}'.format(len(self.routers), len(self.interfaces)))
            for line in pb.iterator(filter(lambda x: x[0] != '#', f)):
                address = line.strip()
                if not check or address not in self.interfaces:
                    nid = 'M{}'.format(rid)
                    rid += 1
                    self.create_objs(ip2as, as2org, nid, [address])

    def create_missing_node(self, address, rid, ip2as, as2org):
        nid = 'M{}'.format(rid)
        rid += 1
        router = Router(nid)
        self.routers[nid] = router
        asn = ip2as[address]
        org = as2org[asn]
        interface = Interface(address, asn, org, router, add_to_router=True)
        self.interfaces[address] = interface
        return interface

    def identify_neighbors(self, filename, adjacent, increment=500000, chunksize=100, ip2as=None, as2org=None):
        used = 0
        modified = 0
        rid = 1
        pb = Progress(message='Adding neighbors', increment=increment, multiplier=chunksize, callback=lambda: 'Used {:,d} Added {:,d} Modified {:,d}'.format(used, rid, modified))
        for df in pb.iterator(pd.read_csv(filename, chunksize=chunksize)):
            for hop1, hop2, distance, private, suspicious, icmp_type in df.itertuples(index=False, name=None):
                if distance > 0:
                    if distance == 1 and (hop1, hop2) not in adjacent:
                        modified += 1
                        distance = 10
                    pred = self.interfaces.get(hop1)
                    if not pred:
                        pred = self.create_missing_node(hop1, rid, ip2as, as2org)
                        rid += 1
                    succ = self.interfaces.get(hop2)
                    if not succ:
                        succ = self.create_missing_node(hop2, rid, ip2as, as2org)
                        rid += 1
                    if pred.router != succ.router:
                        used += 1
                        if distance == 1:
                            if icmp_type == 11:
                                pred.add_succ(succ, Priority.next_hop, Type.next_hop)
                                succ.add_pred(pred, Priority.next_hop, Type.next_hop)
                            elif icmp_type == 0:
                                pred.add_succ(succ, Priority.unreliable, Type.echo)
                                succ.add_pred(pred, Priority.unreliable, Type.echo)
                            elif icmp_type == 3:
                                pred.add_succ(succ, Priority.unreliable, Type.unreachable)
                                succ.add_pred(pred, Priority.unreliable, Type.unreachable)
                            else:
                                raise Exception('Unexpected ICMP Type {}'.format(icmp_type))
                        elif pred.org == succ.org:
                            pred.add_succ(succ, Priority.next_hop, Type.same_as)
                            succ.add_pred(pred, Priority.next_hop, Type.same_as)
                        else:
                            if icmp_type == 11:
                                pred.add_succ(succ, Priority.multi, Type.next_hop)
                                succ.add_pred(pred, Priority.multi, Type.next_hop)
                            elif icmp_type == 0:
                                pred.add_succ(succ, Priority.multi, Type.echo)
                                succ.add_pred(pred, Priority.multi, Type.echo)
                            elif icmp_type == 3:
                                pred.add_succ(succ, Priority.multi, Type.unreachable)
                                succ.add_pred(pred, Priority.multi, Type.unreachable)
                            else:
                                raise Exception('Unexpected ICMP Type {}'.format(icmp_type))

    def othersides(self, additional_addresses=None):
        status('Converting addresses')
        if additional_addresses is None:
            additional_addresses = set()
        ipnums = {struct.unpack("!L", socket.inet_aton(addr.strip()))[0] for addr in chain(self.interfaces, additional_addresses)}
        finish_status('Addresses {:,d}'.format(len(ipnums)))
        found = 0
        pb = Progress(len(self.interfaces), 'Determining other sides', increment=1000000, callback=lambda: 'Found {:,d}'.format(found))
        seen = set()
        for interface in pb.iterator(self.interfaces.values()):
            if interface not in seen:
                otherside_address = determine_otherside(interface.address, ipnums)
                otherside = self.interfaces.get(otherside_address)
                if otherside:
                    otherside = self.interfaces[otherside_address]
                    interface.otherside = otherside
                    otherside.otherside = interface
                    seen.add(interface.address)
                    seen.add(otherside.address)
                    found += 2


def determine_otherside(address: str, all_interfaces: set) -> str:
    """
    Attempts to determine if an interface address in assigned from a /30 or /31 prefix.
    :param address: IPv4 interface address in dot notation
    :param all_interfaces: All known IPv4 interface addresses already converted to integers
    :return: IPv4 address in dot notation
    """
    ip = struct.unpack("!L", socket.inet_aton(address))[0]
    remainder = ip % 4
    network_address = ip - remainder
    broadcast_address = network_address + 3
    if remainder == 0:  # Definitely /31
        otherside = ip + 1
    elif remainder == 3:  # Definitely /31
        otherside = ip - 1
    elif network_address in all_interfaces or broadcast_address in all_interfaces:
        # Definitely /31 because either the network address or broadcast address was seen in interfaces
        # It's either 1 from the network address or 1 from the broadcast address
        otherside = network_address if remainder == 1 else broadcast_address
    else:
        # It's between the network and broadcast address
        # We can't be sure if it's a /30 or /31, so we assume it's a /30
        otherside = (ip + 1) if remainder == 1 else (ip - 1)
    return socket.inet_ntoa(struct.pack('!L', otherside))


def reset(objs, interfaces=True, routers=True, increment=1000000):
    if interfaces:
        pb = Progress(len(objs.interfaces), 'Resetting interfaces', increment=increment)
        for interface in pb.iterator(objs.interfaces.values()):
            interface.pred = set()
            interface.pred_type = Priority.none
            interface.succ = set()
            interface.succ_type = Priority.none
    if routers:
        pb = Progress(len(objs.routers), 'Resetting routers', increment=increment)
        for router in pb.iterator(objs.routers.values()):
            router.succ = defaultdict(Counter)
            router.succ_type = Priority.none
