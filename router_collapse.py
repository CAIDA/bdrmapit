from collections import defaultdict, Counter

from edge import Priority


class Router:

    # __slots__ = ['id', 'asn', 'org', 'interfaces', 'succ', 'succ_type']

    def __init__(self, rid):
        self.id = rid
        self.asn = 0
        self.org = '0'
        self.interfaces = []
        self.succ = defaultdict(Counter)
        self.succ_type = Priority.none

    def __hash__(self):
        return hash(self.id)

    def __repr__(self):
        return 'Router({})'.format(self.id)

    def add_interface(self, interface):
        self.interfaces.append(interface)

    def add_succ(self, interface, successor, priority, ptype):
        self.succ[(successor, priority, ptype)][interface.asn] += 1
        if priority < self.succ_type:
            self.succ_type = priority

    @property
    def isinterface(self):
        return False

    @property
    def isrouter(self):
        return True
