from as2org cimport AS2Org
from bgp.bgp cimport BGP
from graph.hybrid_graph cimport HybridGraph
from updates_dict cimport Updates


cdef class Bdrmapit:

    def __init__(self, HybridGraph graph, AS2Org as2org, BGP bgp, Updates lhupdates = None, Updates updates = None,
                 int iteration = 0, int step = 3):
        self.graph = graph
        self.as2org = as2org
        self.bgp = bgp
        self.iteration = iteration
        self.lhupdates = Updates() if lhupdates is None else lhupdates
        self.updates = Updates() if updates is None else updates
        self.step = step
