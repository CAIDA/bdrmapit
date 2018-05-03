import heapq as hq
from collections import Counter
from typing import List, Set

from graph.bdrmapit cimport Bdrmapit
from graph.interface cimport Interface
from graph.router cimport Router
from utils.progress import Progress

from utils.utils import peek

NOTIMPLEMENTED = 0
NODEST = 1
MODIFIED = 3
SINGLE = 4
SINGLE_MODIFIED = 5
HEAPED = 6
HEAPED_MODIFIED = 7


def heaptest(bdrmapit: Bdrmapit, rdests: Set[int], interfaces: List[Interface]):
    heap = []
    for a in rdests:
        hq.heappush(heap, (bdrmapit.bgp.conesize[a], -a, a))
    original_min = heap[0][-1]
    while heap:
        dest = hq.heappop(heap)[-1]
        for i in interfaces:
            if i.asn == dest or bdrmapit.bgp.rel(i.asn, dest):
                return dest
    return original_min


def annotate(bdrmapit: Bdrmapit, router: Router):
    utype = -1
    rdests = bdrmapit.graph.modified_router_dests[router]
    interfaces = bdrmapit.graph.router_interfaces[router]
    if len(rdests) == 0 or all(dest <= 0 for dest in rdests):
        if len(interfaces) == 1 or len({i.asn for i in interfaces}):
            dest = interfaces[0].asn
            utype = NODEST
        else:
            print(router.name, Counter(i.asn for i in interfaces))
            dest = -1
            utype = NOTIMPLEMENTED
    else:
        rorgs = {bdrmapit.as2org[d] for d in rdests}
        if len(rorgs) == 1:
            dest = list(rdests)[0]
            if utype == MODIFIED:
                utype = SINGLE_MODIFIED
            else:
                utype = SINGLE
        else:
            ifaces = {interface.asn for interface in interfaces}
            same = [dest for dest in rdests if dest in ifaces]
            rels = [dest for dest in rdests if any(bdrmapit.bgp.rel(i, dest) for i in ifaces)]
            if len(same) == 1:
                return same[0], 8
            if rels:
                asn = max(rels, key=lambda x: (len(bdrmapit.bgp.cone[x] & rdests), -bdrmapit.bgp.conesize[x], x))
                return asn, 9
            dest = heaptest(bdrmapit, rdests, interfaces)
            if utype == MODIFIED:
                utype = HEAPED_MODIFIED
            else:
                utype = HEAPED
        if not any(bdrmapit.bgp.rel(i.asn, dest) for i in interfaces):
            intersection = bdrmapit.bgp.providers[dest] & {a for i in interfaces for a in bdrmapit.bgp.customers[i.asn]}
            if len(intersection) == 1:
                dest = peek(intersection)
    return dest, utype


def annotate_lasthops(bdrmapit: Bdrmapit, routers: List[Router]=None):
    if routers is None:
        routers = bdrmapit.graph.routers_nosucc
    ifs = 0
    ds = 0
    pb = Progress(len(routers), message='Last Hops', increment=100000, callback=lambda: 'Is {:,d} Ds {:,d}'.format(ifs, ds))
    for router in pb.iterator(routers):
        dest, utype = annotate(bdrmapit, router)
        if utype == NODEST:
            ifs += 1
        else:
            ds += 1
        bdrmapit.lhupdates.add_update(router, dest, bdrmapit.as2org[dest], utype)
