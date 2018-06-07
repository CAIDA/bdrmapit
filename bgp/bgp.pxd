from utils.utils cimport DictSet, DictInt

cpdef enum RelType:
    customer = 1
    provider = 2
    peer = 3
    none = 4

cdef class BGP:
    cdef public set customer_rels
    cdef public set peer_rels
    cdef public DictSet customers
    cdef public DictSet providers
    cdef public set relationships
    cdef public DictInt num_customers
    cdef public DictSet peers
    cdef public DictSet cone
    cdef public DictInt conesize

    cpdef bint customer_rel(self, int a, int b) except -1
    cpdef bint provider_rel(self, int a, int b) except -1
    cpdef bint peer_rel(self, int a, int b) except -1
    cpdef bint rel(self, int a, int b) except -1
    cpdef RelType reltype(self, int a, int b) except *