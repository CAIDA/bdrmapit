from collections import namedtuple

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
        self.changes = {}

    def __setitem__(self, key, value):
        if self[key] != value:
            self.changes[key] = value

    def __missing__(self, key):
        return -1, '-1', -1

    def add_update(self, node, asn, org, utype):
        self[node] = (asn, org, utype)

    def advance(self):
        self.update(self.changes)
        self.changes = {}

    def asn(self, node):
        return self[node][ASN]

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

    def org(self, node):
        return self[node][ORG]

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
                    for successor, priority, ptype in sorted(router.succ, key=lambda x: x[1]):
                        if successor not in seen:
                            seen.add(successor)
                            srouter = successor.router
                            rasn, rorg, _ = self.get(srouter, (srouter.asn, srouter.org, -1))
                            far_sides.append(FarSide(successor, rasn, rorg, priority, ptype, srouter))
                    connections.append(NearSide(uasn, uorg, router.interfaces, far_sides))
        return connections

    def routers_iter(self):
        yield from filter(lambda x: x.isrouter, self)


class UpdatesView(Updates):

    def __init__(self, original: Updates, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.original = original

    def __missing__(self, key):
        return self.original[key]
