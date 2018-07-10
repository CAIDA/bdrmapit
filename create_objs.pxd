from as2org cimport AS2Org
from bgp.bgp cimport BGP
from graph.hybrid_graph cimport HybridGraph

cdef class CreateObjs:
    cdef con
    cdef HybridGraph g
    cdef ip2as
    cdef AS2Org as2org
    cdef BGP bgp