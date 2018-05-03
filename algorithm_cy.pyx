from collections import Counter, defaultdict
from typing import DefaultDict

from graph.bdrmapit cimport Bdrmapit
from graph.interface cimport Interface
from graph.router cimport Router
from updates_dict cimport Updates
from utils.progress import Progress
from utils.utils cimport max_num, peek


cpdef tuple graph_refinement(Bdrmapit bdrmapit, list routers, list interfaces, int iterations = -1, list previous_updates = None):
    cdef rupdates = Updates()
    cdef iupdates = Updates()
    cdef iteration = 0
    if previous_updates is None:
        previous_updates = []
    while iterations < 0 or iteration < iterations:
        Progress.message('********** Iteration {:,d} **********'.format(iteration))
        rupdates = annotate_routers(bdrmapit, rupdates, iupdates, routers=routers)
        iupdates = annotate_interfaces(bdrmapit, rupdates, iupdates, interfaces=interfaces)
        if (rupdates, iupdates) in previous_updates:
            break
        previous_updates.append((rupdates, iupdates))
        iteration += 1
    return rupdates, iupdates


cdef Updates annotate_routers(Bdrmapit bdrmapit, Updates rupdates, Updates iupdates, list routers = None, int increment=100000):
    cdef Updates new_updates = rupdates.copy()
    cdef Router router
    cdef int asn
    if routers is None:
        routers = bdrmapit.graph.routers_succ
    pb = Progress(len(routers), 'Annotating routers', increment=increment)
    for router in pb.iterator(routers):
        asn = annotate_router(bdrmapit, router, rupdates, iupdates)
        new_updates.add_update(router, asn, bdrmapit.as2org[asn], 2)
    return new_updates


cdef int router_heuristics(Bdrmapit bdrmapit, Router router, Interface isucc, set origins, Updates rupdates, Updates iupdates) except -3:
    cdef Router rsucc
    cdef int rsucc_asn, asn, succ_asn
    cdef set dests
    if isucc.asn == -1:
        if origins:
            return max(origins, key=lambda x: (bdrmapit.bgp.conesize[x], -x))
        return -1
    rsucc = bdrmapit.graph.interface_router[isucc]
    rsucc_asn = get(bdrmapit, rsucc, rupdates)[0]
    if isucc.asn == 0:
        return rsucc_asn
    if isucc.asn in origins:
        return isucc.asn
    if rsucc_asn > 0 and rsucc_asn != isucc.asn:
        if any(asn == rsucc_asn or bdrmapit.bgp.rel(asn, rsucc_asn) for asn in origins):
            dests = bdrmapit.graph.modified_router_dests[router]
            if isucc.asn not in dests:
                return rsucc_asn
    succ_asn = iupdates[isucc][0]
    if succ_asn <= 0 or (rsucc_asn > 0 and succ_asn != rsucc_asn):
        succ_asn = isucc.asn
    return succ_asn


cdef void router_updates(Bdrmapit bdrmapit, set edges, succs: Counter, succ_origins: DefaultDict, succ_ifaces: DefaultDict, Updates rupdates) except *:
    cdef str prefix
    cdef set succ_updates, update_asns, noupdate_asns, prefixes
    cdef list isuccs
    cdef Router rsucc
    cdef Interface isucc
    cdef int update, num, asn, oldasn
    if len(edges) > 1 and len(succs) <= 2:
        succ_updates = set()
        update_asns = set()
        noupdate_asns = set()
        prefixes = set()
        asn_prefix = defaultdict(set)
        for asn, isuccs in succ_ifaces.items():
            if asn in succ_origins[asn]:
                update_asns.add(asn)
                for isucc in isuccs:
                    rsucc = bdrmapit.graph.interface_router[isucc]
                    update = get(bdrmapit, rsucc, rupdates)[0]
                    succ_updates.add(update)
                    prefix = isucc.address.rpartition('.')[0]
                    prefixes.add(prefix)
                    asn_prefix[asn].add(prefix)
            else:
                noupdate_asns.add(asn)
        if len(noupdate_asns) == 1:
            if all(len(asn_prefix[asn]) == 1 for asn in update_asns):
                update = peek(noupdate_asns)
                if bdrmapit.bgp.conesize[update] < 30:
                    if all(bdrmapit.bgp.provider_rel(origin, update) or not bdrmapit.bgp.rel(origin, update) for origin in update_asns):
                        for oldasn in update_asns:
                            num = succs.pop(oldasn)
                            succs[update] = num
                            succ_origins[update].update(succ_origins.pop(oldasn))


cdef int router_exceptions(Bdrmapit bdrmapit, succs: Counter, ifaces: Counter, votes: Counter, succ_origins: DefaultDict) except -3:
    cdef int asn, total, iasn, num, s, lowest
    cdef set intersection, origins
    if len(succs) == 1:
        asn = peek(succs)
        if len(ifaces) > 1:
            if sum(1 for iasn in ifaces if iasn == asn or bdrmapit.bgp.rel(iasn, asn)) > 1:
                return asn
        total = 0
        for iasn, num in ifaces.items():
            if bdrmapit.bgp.provider_rel(iasn, asn):
                total += num
        if total > 0 and total > succs[asn] >= total / 3:
            return asn
        if not any(iasn == asn or bdrmapit.bgp.rel(iasn, asn) for iasn in ifaces):
            if succs[asn] >= sum(ifaces.values()) / 3:
                intersection = {a for o in ifaces for a in bdrmapit.bgp.customers[o]} & bdrmapit.bgp.providers[asn]
                if len(intersection) == 1:
                    asn = peek(intersection)
                    return asn
                return asn
    if succs and all(s in ifaces for s in succs):
        lowest = min(succs, key=lambda x: (bdrmapit.bgp.conesize[x], -x))
        origins = succ_origins[lowest]
        if lowest not in origins or (len(origins) > 1):
            if (any(bdrmapit.bgp.provider_rel(iasn, lowest) for iasn in origins) or all(not bdrmapit.bgp.provider_rel(iasn, lowest) for iasn in origins)) and votes[lowest] >= (sum(votes.values()) - votes[lowest]) / 3:
                return lowest
    return -1


cdef int router_tiebreaker(Bdrmapit bdrmapit, list asns, ifaces: Counter) except -3:
    cdef int iasn, sasn, asn
    if len(asns) > 2:
        for iasn in ifaces:
            if all(iasn == sasn or bdrmapit.bgp.peer_rel(iasn, sasn) or bdrmapit.bgp.customer_rel(iasn, sasn) for sasn in asns):
                return iasn
    asn = min(asns, key=lambda x: (bdrmapit.bgp.conesize[x], -x))
    return asn


cdef int annotate_router(Bdrmapit bdrmapit, Router router, Updates rupdates, Updates iupdates) except -3:
    cdef set edges, intersection
    cdef int rtype, succ_asn, exception, asn, o
    cdef Interface isucc
    cdef list asns, rels
    edges, rtype = get_edges(bdrmapit, router)
    succs = Counter()
    succ_origins = defaultdict(set)
    succ_ifaces = defaultdict(list)
    for isucc in edges:
        origins = {o for o in get_origins(bdrmapit, router, isucc, rtype) if o > 0}
        succ_asn = router_heuristics(bdrmapit, router, isucc, origins, rupdates, iupdates)
        if succ_asn > 0:
            succs[succ_asn] += 1
            succ_origins[succ_asn].update(origins)
            succ_ifaces[succ_asn].append(isucc)
    router_updates(bdrmapit, edges, succs, succ_origins, succ_ifaces, rupdates)
    ifaces = Counter(i.asn for i in bdrmapit.graph.router_interfaces[router] if i.asn > 0)
    votes = succs + ifaces
    asns = max_num(votes, key=votes.__getitem__)
    exception = router_exceptions(bdrmapit, succs, ifaces, votes, succ_origins)
    if exception > 0:
        return exception
    if len(asns) > 1:
        asn = router_tiebreaker(bdrmapit, asns, ifaces)
    elif len(asns) == 1:
        asn = asns[0]
    else:
        return 0
    origins = succ_origins[asn]
    if not origins:
        origins = ifaces
    if asn not in origins and not any(bdrmapit.bgp.rel(o, asn) for o in origins):
        rels = [s for s in votes if s in succ_origins[s] or any(bdrmapit.bgp.rel(s, o) for o in succ_origins[s])]
        intersection = {a for o in origins for a in bdrmapit.bgp.customers[o]} & bdrmapit.bgp.providers[asn]
        if rels:
            asns = max_num(rels, key=votes.__getitem__)
            asn = min(asns, key=lambda x: (bdrmapit.bgp.conesize[x], -x))
        else:
            if len(intersection) == 1:
                asn = peek(intersection)
        return asn
    return asn


cdef Updates annotate_interfaces(Bdrmapit bdrmapit, Updates rupdates, Updates iupdates, list interfaces = None):
    cdef Updates new_updates = iupdates.copy()
    cdef Interface interface
    cdef int asn
    if interfaces is None:
        interfaces = bdrmapit.graph.interfaces_pred
    pb = Progress(len(interfaces), 'Adding links', increment=200000)
    for interface in pb.iterator(interfaces):
        if interface.asn >= 0:
            asn = annotate_interface(bdrmapit, interface, rupdates)
            new_updates.add_update(interface, asn, bdrmapit.as2org[asn], bdrmapit.step)
    return new_updates


cdef int annotate_interface(Bdrmapit bdrmapit, Interface interface, Updates rupdates) except -3:
    cdef Interface ipred
    cdef int asn
    cdef Router rpred
    cdef list asns
    cdef set edges = set(bdrmapit.graph.inexthop.get(interface))
    votes = Counter()
    for ipred in edges:
        rpred = bdrmapit.graph.interface_router[ipred]
        if interface.org == ipred.org:
            asn = ipred.asn
        else:
            asn, _, _ = rupdates[rpred]
            if asn == -1:
                asn = ipred.asn
        votes[asn] += 1
    if len(votes) == 1:
        return peek(votes)
    asns = max_num(votes, key=votes.__getitem__)
    asn = min(asns, key=lambda x: (bdrmapit.bgp.conesize[x], -x))
    return asn


cdef tuple get(Bdrmapit bdrmapit, Router r, Updates updates):
    cdef tuple result = bdrmapit.lhupdates[r]
    if result[0] == -1:
        return updates[r]
    return result


cpdef tuple get_edges(Bdrmapit bdrmapit, Router router):
    cdef set edges
    cdef int rtype
    edges = bdrmapit.graph.rnexthop.get(router)
    if edges:
        rtype = 1
    else:
        edges = bdrmapit.graph.recho.get(router)
        if edges:
            rtype = 2
        else:
            edges = bdrmapit.graph.rmulti[router]
            rtype = 3
    return edges, rtype


cdef set get_origins(Bdrmapit bdrmapit, Router router, Interface interface, int rtype):
    if rtype == 1:
        return bdrmapit.graph.rnh_ases[router, interface]
    elif rtype == 2:
        return bdrmapit.graph.re_ases[router, interface]
    else:
        return bdrmapit.graph.rm_ases[router, interface]
