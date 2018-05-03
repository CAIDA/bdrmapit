from bgp.bgp cimport BGP
from graph.hybrid_graph cimport HybridGraph
from as2org cimport AS2Org
from updates_dict cimport Updates

cdef class Bdrmapit:
    cdef public HybridGraph graph
    cdef public AS2Org as2org
    cdef public BGP bgp
    cdef public int iteration, step
    cdef public Updates lhupdates, updates
