from edge import Priority


class Interface:

    __slots__ = ['address', 'asn', 'org', 'router', 'otherside', 'pred', 'pred_type', 'succ', 'succ_type']

    def __init__(self, address, asn, org, router, otherside=None, add_to_router=True):
        self.address = address
        self.asn = asn
        self.org = org
        self.router = router
        self.otherside = otherside
        self.pred = set()
        self.pred_type = Priority.none
        self.succ = set()
        self.succ_type = Priority.none
        if add_to_router:
            self.router.add_interface(self)

    def __hash__(self):
        return hash(self.address)

    def __repr__(self):
        return 'Interface({})'.format(self.address)

    def add_pred(self, interface, priority, ptype):
        self.pred.add((interface, priority, ptype))
        if priority < self.pred_type:
            self.pred_type = priority

    def add_succ(self, interface, priority, ptype):
        self.succ.add((interface, priority, ptype))
        self.router.add_succ(self, interface, priority, ptype)
        if priority < self.succ_type:
            self.succ_type = priority

    @property
    def isinterface(self):
        return True

    @property
    def isrouter(self):
        return False
