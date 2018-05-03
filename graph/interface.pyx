from graph.router cimport Router


cdef class Interface(Router):

    def __init__(self, str address, int asn, str org):
        super().__init__(address)
        self.address = address
        self.asn = asn
        self.org = org

    def __repr__(self):
        return 'Interface<{}>'.format(self.name)
