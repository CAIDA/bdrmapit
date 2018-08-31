import heapq as hq
from collections import Counter
from typing import List, Set

from graph.bdrmapit import Bdrmapit
from graph.interface import Interface
from graph.router import Router
from log import Log
from utils.progress import Progress

from utils.utils import peek

log = Log()

NOTIMPLEMENTED = 0
NODEST = 1
MODIFIED = 3
SINGLE = 4
SINGLE_MODIFIED = 5
HEAPED = 6
HEAPED_MODIFIED = 7
MISSING_NOINTER = 10
MISSING_INTER = 9


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


def nodests(bdrmapit: Bdrmapit, router: Router, interfaces: List[Interface]):
    if len(interfaces) == 1 or len({i.asn for i in interfaces}):
        dest = interfaces[0].asn
        utype = NODEST
        return dest, utype
    else:
        print(router.name, Counter(i.asn for i in interfaces))
        dest = -1
        utype = NOTIMPLEMENTED
        return dest, utype


def annotate(bdrmapit: Bdrmapit, router: Router):
    utype = -1
    rdests = bdrmapit.graph.modified_router_dests[router]
    log.debug(rdests)
    interfaces = bdrmapit.graph.router_interfaces[router]
    if len(rdests) == 0 or all(dest <= 0 for dest in rdests):
        return nodests(bdrmapit, router, interfaces)
    rorgs = {bdrmapit.as2org[d] for d in rdests}
    if len(rorgs) == 1:
        dest = list(rdests)[0]
        utype = SINGLE
    else:
        ifaces = {interface.asn for interface in interfaces}
        log.debug('Ifaces: {}'.format(ifaces))
        same = [dest for dest in rdests if dest in ifaces]
        rels = [dest for dest in rdests if any(bdrmapit.bgp.rel(i, dest) for i in ifaces)]
        log.debug('Same: {}'.format(same))
        log.debug('Rels: {}'.format(rels))
        if len(same) == 1:
            return same[0], 8
        if rels:
            asn = min(rels, key=lambda x: (bdrmapit.bgp.conesize[x], -x))
            # asn = max(rels, key=lambda x: (len(bdrmapit.bgp.cone[x] & rdests), -bdrmapit.bgp.conesize[x], x))
            return asn, 9
        dest = heaptest(bdrmapit, rdests, interfaces)
        if utype == MODIFIED:
            utype = HEAPED_MODIFIED
        else:
            utype = HEAPED
    iasns = Counter(i.asn for i in interfaces if i.asn > 0)
    if iasns and not dest in iasns and not any(bdrmapit.bgp.rel(iasn, dest) for iasn in iasns):
        intersection = bdrmapit.bgp.providers[dest] & {a for i in interfaces for a in bdrmapit.bgp.customers[i.asn]}
        if len(intersection) == 1:
            dest = peek(intersection)
            return dest, MISSING_INTER
        log.debug(bdrmapit.bgp.providers[dest] & {a for i in interfaces for a in bdrmapit.bgp.peers[i.asn]})
        c = Counter(i.asn for i in interfaces if i.asn > 0)
        if c:
            return max(c, key=lambda x: (c[x], -bdrmapit.bgp.conesize[x], -x)), MISSING_NOINTER
        return dest, MISSING_NOINTER
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
    bdrmapit.lhupdates.advance()
