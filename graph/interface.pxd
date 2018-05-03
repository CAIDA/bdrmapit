from graph.router cimport Router

cdef class Interface(Router):
    cdef public str address, org
    cdef public int asn
