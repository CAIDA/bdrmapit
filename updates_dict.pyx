from collections import Sequence, namedtuple

import pandas as pd

ASN = 0
ORG = 1
UTYPE = 2

cdef class Updates(dict):
    @classmethod
    def from_updates(cls, updates, name=None):
        if name is None:
            name = updates.name
        copy = cls(updates, name=name)
        return copy

    def __init__(self, *args, name=None, **kargs):
        super().__init__(*args, **kargs)
        self.name = name

    def __missing__(self, key):
        return -1, '-1', -1

    cpdef void add_update(self, Router node, int asn, str org, int utype) except *:
        self[node] = (asn, org, utype)

    cpdef int asn(self, Router node) except -1:
        return self[node][ASN]

    cpdef void bdrmap_output(self, str filename, asn=None, str org=None, routers=None) except *:
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

    cpdef links(self, interfaces, asn=None, org=None):
        df = self.results(interfaces)
        return df[(df.Org == org) | (df.ConnOrg == org)][['Interface', 'ASN', 'ConnASN']].copy()

    cpdef str org(self, Router node):
        return self[node][ORG]

    cpdef results(self, list interfaces, bint updates_only=False, list networks=None):
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

    def router_connections(self, list routers, asn=None, str org=None):
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
