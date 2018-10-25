from as2org import AS2Org
from bgp.bgp import BGP
from graph.hybrid_graph import HybridGraph
from updates_dict import Updates, UpdatesView


class Bdrmapit:

    def __init__(self, graph: HybridGraph, as2org: AS2Org, bgp: BGP, lhupdates: Updates = None):
        self.graph = graph
        self.as2org = as2org
        self.bgp = bgp
        if lhupdates is None:
            self.lhupdates = Updates()
        else:
            self.lhupdates = UpdatesView(lhupdates)
