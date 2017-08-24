from collections import Sequence, namedtuple

import pandas as pd

from edge import Priority, Type
from utils.utils import unique_everseen

ASN = 0
ORG = 1
UTYPE = 2


class Updates(dict):
    @classmethod
    def from_updates(cls, updates, name=None):
        if name is None:
            name = updates.name
        copy = cls(updates, name=name)
        return copy

    def __init__(self, *args, name=None, **kargs):
        super().__init__(*args, **kargs)
        self.name = name

    def add_update(self, node, asn, org, utype):
        self[node] = (asn, org, utype)

    def asn(self, node):
        return self[node][ASN]

    def asn_default(self, node, default=None):
        try:
            return self.asn(node)
        except KeyError:
            return default

    def bdrmap_output(self, filename, asn=None, org=None, routers=None):
        connections = self.router_connections(routers, asn=asn, org=org)
        with open(filename, 'w') as f:
            for rasn, rorg, interfaces, far_sides in connections:
                f.write('owner {} (something)\n'.format(asn))
                for interface in interfaces:
                    f.write(interface.address)
                    if interface.pred or interface.succ:
                        f.write('*')
                    f.write('\n')
                for successor, sasn, sorg, priority, ptype, router in far_sides:
                    f.write(' {} {} {} {}*'.format(sasn, priority.name, ptype.name, successor.address))
                    for interface in successor.router.interfaces:
                        if interface != successor:
                            f.write(' {}'.format(interface.address))
                            if interface.pred or interface.succ:
                                f.write('*')
                    f.write('\n')
                f.write('\n')

    def copy(self):
        return Updates.from_updates(self)

    def delete(self, node):
        del self[node]

    def isupdated(self, node):
        try:
            return node.org != self[node][ORG]
        except KeyError:
            return False

    def links(self, interfaces, asn=None, org=None):
        df = self.results(interfaces)
        return df[(df.Org == org) | (df.ConnOrg == org)][['Interface', 'ASN', 'ConnASN']].copy()

    def mapping(self, node):
        asn, org, _ = self[node]
        return asn, org

    def mapping_default(self, node):
        try:
            return self.mapping(node)
        except KeyError:
            return node.asn, node.org

    def near_side(self, filename, routers, asn=None, org=None, include_all=False):
        connections = self.router_connections(routers, asn=asn, org=org)
        with open(filename, 'w') as f:
            for asn, org, interfaces, far_sides in connections:
                fasns = list(map(str, sorted(unique_everseen(fs.asn for fs in far_sides if fs.asn > 0 and fs.priority < Priority.multi and fs.type != Type.same_as))))
                if len(fasns) == 1 and asn in fasns:
                    continue
                if fasns or include_all:
                    f.write(','.join(i.address for i in interfaces))
                    f.write('|')
                    f.write(','.join(fasns))
                    f.write('\n')

    def org(self, node):
        return self[node][ORG]

    def org_default(self, node, default=None):
        try:
            return self.org(node)
        except KeyError:
            return default

    def results(self, interfaces, updates_only=False, networks=None):
        if networks and not isinstance(networks, Sequence) or isinstance(networks, str):
            networks = {networks}
        elif networks:
            networks = set(networks)
        rows = []
        for interface in interfaces:
            router = interface.router
            iasn = router.asn
            iorg = router.org
            asn, org, router_utype = self.get(router, (iasn, iorg, -1))
            if router_utype == -1 and updates_only:
                continue
            if interface.asn != 0:
                if interface.org != org:
                    conn_asn = interface.asn
                    conn_org = interface.org
                    interface_utype = -2
                else:
                    conn_asn, conn_org, interface_utype = self.get(interface, (interface.asn, interface.org, -1))
            else:
                conn_asn, conn_org, interface_utype = self.get(interface, (interface.asn, interface.org, -1))
            if not networks or asn in networks or conn_asn in networks:
                rows.append(
                    [router.id, interface.address, asn, org, conn_asn, conn_org, router_utype, interface_utype])
        return pd.DataFrame(rows, columns=[
            'Router', 'Interface', 'ASN', 'Org', 'ConnASN', 'ConnOrg', 'RUpdate', 'IUpdate'])

    def router_connections(self, routers, asn=None, org=None):
        NearSide = namedtuple('NearSide', ['asn', 'org', 'interfaces', 'far_sides'])
        FarSide = namedtuple('FarSide', ['address', 'asn', 'org', 'priority', 'type', 'router'])
        connections = []
        for router in routers:
            if router.isrouter:
                uasn, uorg, uround = self.get(router, (router.asn, router.org, -1))
                if uasn == asn or uorg == org:
                    far_sides = []
                    seen = set()
                    # if any(i.address == '4.71.122.6' for i in router.interfaces):
                    #     print(uasn, uorg, uround, router, self[router])
                    for successor, priority, ptype in sorted(router.succ, key=lambda x: x[1]):
                        if successor not in seen:
                            seen.add(successor)
                            srouter = successor.router
                            rasn, rorg, _ = self.get(srouter, (srouter.asn, srouter.org, -1))
                            # if successor.address == '206.126.236.137':
                            #     print((rasn, rorg))
                            far_sides.append(FarSide(successor, rasn, rorg, priority, ptype, srouter))
                    connections.append(NearSide(uasn, uorg, router.interfaces, far_sides))
        return connections

    def routers_iter(self):
        yield from filter(lambda x: x.isrouter, self)