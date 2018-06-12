from utils.utils cimport File2, DictSet, DictInt
from bgp.bgp cimport RelType


cdef class BGP:
    def __init__(self, rels=None, cone=None):
        cdef int provider, customer, rel
        cdef str c
        cdef set customers
        cdef list splits
        self.customer_rels = set()
        self.peer_rels = set()
        self.customers = DictSet()
        self.providers = DictSet()
        self.relationships = set()
        self.num_customers = DictInt()
        self.peers = DictSet()
        self.cone = DictSet()
        self.conesize = DictInt()
        if rels is not None:
            with File2(rels) as f:
                for line in f:
                    if not line[0] == '#':
                        splits = line.rstrip().split('|')
                        provider = int(splits[0])
                        customer = int(splits[1])
                        rel = int(splits[2])
                        self.relationships.add((provider, customer))
                        self.relationships.add((customer, provider))
                        if rel == -1:
                            self.customers[provider].add(customer)
                            self.providers[customer].add(provider)
                            self.customer_rels.add((customer, provider))
                            self.num_customers[provider] += 1
                        else:
                            self.peers[provider].add(customer)
                            self.peers[customer].add(provider)
                            self.peer_rels.add((provider, customer))
                            self.peer_rels.add((customer, provider))
        if cone is not None:
            with File2(cone) as f:
                for line in filter(lambda x: x[0] != '#', f):
                    splits = line.split()
                    provider = int(splits[0])
                    customers = self.cone[provider]
                    for c in splits[1:]:
                        customer = int(c)
                        if customer != provider:
                            customers.add(customer)
                    self.conesize[int(provider)] = len(customers)
            self.customers.finalize()
            self.providers.finalize()
            self.num_customers.finalize()
            self.peers.finalize()
            self.cone.finalize()
            self.conesize.finalize()

    cpdef bint customer_rel(self, int a, int b) except -1:
        return (a, b) in self.customer_rels

    cpdef bint provider_rel(self, int a, int b) except -1:
        return (b, a) in self.customer_rels

    cpdef bint peer_rel(self, int a, int b) except -1:
        return (a, b) in self.peer_rels

    cpdef bint rel(self, int a, int b) except -1:
        return (a, b) in self.relationships
    
    cpdef RelType reltype(self, int a, int b) except *:
        if self.customer_rel(a, b):
            return RelType.customer
        elif self.provider_rel(a, b):
            return RelType.provider
        elif self.peer_rel(a, b):
            return RelType.peer
        else:
            return RelType.none

    def __contains__(self, item):
        return item in self.cone
