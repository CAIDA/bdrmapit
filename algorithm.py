from collections import Counter, defaultdict
from typing import List, Tuple, Set, Dict

from graph.bdrmapit import Bdrmapit
from graph.interface import Interface
from graph.router import Router
from log import Log
from updates_dict import Updates
from utils.progress import Progress
from utils.utils import peek, max_num


log = Log()

REALLOCATED_PREFIX = 500
REALLOCATED_DEST = 1000
SINGLE_SUCC_ORIGIN = 10
SINGLE_SUCC_4 = 11
SUCC_ORIGIN_INTER = 12
SUCC_ORIGIN_CUST = 13
REMAINING_4 = 14
IUPDATE = 15
ALLPEER_SUCC = 16
ALLPEER_ORIGIN = 17
IASN_SUCC_HALF = 18
ALLRELS = 19
VOTE_SINGLE = 50
VOTE_TIE = 70
SINGLE_SUCC_RASN = 15
HIDDEN_INTER = 100
HIDDEN_NOINTER = 200


def graph_refinement(bdrmapit: Bdrmapit, routers: List[Router], interfaces: List[Interface], iterations: int = -1, previous_updates: List[Tuple[Updates, Updates]] = None) -> Tuple[Updates, Updates]:
    rupdates = Updates()
    iupdates = Updates()
    iteration = 0
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


def router_heuristics(bdrmapit: Bdrmapit, router: Router, isucc: Interface, origins: Set[int], rtype: int, rupdates: Updates, iupdates: Updates):
    if isucc.asn == -1:
        # return max(origins, key=lambda x: (bdrmapit.bgp.conesize[x], -x)) if origins else -1
        return -1
    rsucc = bdrmapit.graph.interface_router[isucc]
    rsucc_asn = get(bdrmapit, rsucc, rupdates)[0]
    succ_asn = iupdates[isucc][0]
    log.debug('\tASN={}, RASN={}, IASN={}'.format(isucc.asn, rsucc_asn, succ_asn))
    if isucc.asn == 0:
        return rsucc_asn
    if isucc.asn in origins:
        return isucc.asn
    if rsucc_asn > 0 and rsucc_asn != isucc.asn:
        log.debug('\tThird party: Router={}, RASN={}'.format(rsucc.name, rsucc_asn))
        if not any(isucc.org == bdrmapit.as2org[asn] for asn in origins):
            if any(asn == rsucc_asn or bdrmapit.bgp.rel(asn, rsucc_asn) for asn in origins):
                dests = bdrmapit.graph.modified_router_dests[router]
                if isucc.asn not in dests:
                    return rsucc_asn
    if rtype == 2 or succ_asn <= 0 or (rsucc_asn > 0 and isucc.asn != rsucc_asn):
        return isucc.asn
    return succ_asn


def reallocated_test(bdrmapit: Bdrmapit, oasn, newasn):
    conesize = bdrmapit.bgp.conesize[newasn]
    log.debug('Reallocated Test: conesize={} < 5 and conesize={} < oldcone={} and not peer_rel({}, {}) = {}'.format(conesize, conesize, bdrmapit.bgp.conesize[oasn], newasn, oasn, bdrmapit.bgp.peer_rel(newasn, oasn)))
    return conesize <= 3 and conesize < bdrmapit.bgp.conesize[oasn] and not bdrmapit.bgp.peer_rel(newasn, oasn)


def reallocated(bdrmapit: Bdrmapit, router: Router, edges: Set[Interface], rtype: int, rupdates: Updates, succs: Counter, succ_origins: Dict[int, Set], iasns: Counter):
    if len(edges) > 1:
        same = defaultdict(list)
        for s in edges:
            if s.asn in get_origins(bdrmapit, router, s, rtype):
                same[s.asn].append(s)
        if log.isdebug():
            log.debug('Same: {}'.format({k: [i.address for i in v] for k, v in same.items()}))
        for oasn, isuccs in same.items():
            if len(isuccs) > 1:
                prefixes = {s.address.rpartition('.')[0] for s in isuccs}
                log.debug('Prefixes: {}'.format(prefixes))
                if len(prefixes) == 1:
                    rsuccs = {bdrmapit.graph.interface_router[s] for s in isuccs}
                    if all(get(bdrmapit, rsucc, rupdates)[-1] < REALLOCATED_PREFIX for rsucc in rsuccs):
                        rasns = {get(bdrmapit, rsucc, rupdates)[0] for rsucc in rsuccs}
                        log.debug('RASNs: {}'.format(rasns))
                        if len(rasns) > 1 or oasn in rasns:
                            mrdests = bdrmapit.graph.modified_router_dests[router]
                            log.debug('Modified Dests: {}'.format(mrdests))
                            if len(mrdests) == 1:
                                rasns = mrdests
                        if len(rasns) == 1:
                            newasn = peek(rasns)
                            if newasn > 0 and newasn != oasn:
                                if reallocated_test(bdrmapit, oasn, newasn):
                                    num = succs.pop(oasn)
                                    succs[newasn] = num
                                    succ_origins[newasn] = succ_origins[oasn]
                                    return REALLOCATED_PREFIX
    return 0


def hidden_asn(bdrmapit: Bdrmapit, iasns, asn, utype):
    intersection = {a for o in iasns for a in bdrmapit.bgp.customers[o]} & bdrmapit.bgp.providers[asn]
    if len(intersection) == 1:
        asn = peek(intersection)
        log.debug('Hidden: {}'.format(asn))
        return asn, HIDDEN_INTER + utype
    log.debug('Missing: {}-{}'.format(iasns, asn))
    return asn, HIDDEN_NOINTER + utype
    # return max(iasns, key=lambda x: (iasns[x], -bdrmapit.bgp.conesize[x], x)), HIDDEN_NOINTER + utype


def conetest(bdrmapit: Bdrmapit, asn):
    org = bdrmapit.as2org[asn]
    cone = bdrmapit.bgp.cone[asn]
    return sum(1 for customer in cone if bdrmapit.as2org[customer] != org)


def annotate_router(bdrmapit: Bdrmapit, router: Router, rupdates: Updates, iupdates: Updates):
    utype = 0
    edges, rtype = get_edges(bdrmapit, router)
    log.debug('Edges={}, Rtype={}'.format(len(edges), rtype))
    succs = Counter()
    succ_origins = defaultdict(set)
    for isucc in edges:
        origins = {o for o in get_origins(bdrmapit, router, isucc, rtype) if o > 0}
        log.debug('Succ={}, ASN={}, Origins={}'.format(isucc.address, isucc.asn, origins))
        succ_asn = router_heuristics(bdrmapit, router, isucc, origins, rtype, rupdates, iupdates)
        log.debug('Heuristic: {}'.format(succ_asn))
        if succ_asn > 0:
            succs[succ_asn] += 1
            succ_origins[succ_asn].update(origins)
    log.debug('Succs: {}'.format(succs))
    iasns = Counter(i.asn for i in bdrmapit.graph.router_interfaces[router] if i.asn > 0)
    utype += reallocated(bdrmapit, router, edges, rtype, rupdates, succs, succ_origins, iasns)
    log.debug('IASNS: {}'.format(iasns))
    if len(succs) == 1 or len({bdrmapit.as2org[sasn] for sasn in succs}) == 1:
        sasn = peek(succs) if len(succs) == 1 else max(succs, key=lambda x: (bdrmapit.bgp.conesize[x], -x))
        if sasn in iasns:
            return sasn, utype + SINGLE_SUCC_ORIGIN
        if succs[sasn] > sum(iasns.values()) / 4:
            for iasn in succ_origins[sasn]:
                if bdrmapit.bgp.customer_rel(sasn, iasn):
                    log.debug('Provider: {}->{}'.format(iasn, sasn))
                    return sasn, utype + SINGLE_SUCC_4
            conesize = bdrmapit.bgp.conesize[sasn]
            if not any(bdrmapit.bgp.rel(iasn, sasn) for iasn in succ_origins[sasn]) and any(bdrmapit.bgp.conesize[iasn] > conesize for iasn in succ_origins[sasn]):
                return hidden_asn(bdrmapit, succ_origins[sasn], sasn, utype)
                # return hidden_asn(bdrmapit, iasns, sasn, utype)
            for isucc in edges:
                sasn2, _, itype = iupdates[isucc]
                rasn = get(bdrmapit, bdrmapit.graph.interface_router[isucc], rupdates)[0]
                if sasn2 == sasn and ((rasn == sasn and itype == 1) or rasn != sasn):
                    return sasn, utype + IUPDATE
            rasns = set()
            for isucc in edges:
                rasn = get(bdrmapit, bdrmapit.graph.interface_router[isucc], rupdates)[0]
                rasns.add(rasn if rasn > 0 else sasn)
            log.debug('RASNS={}, SASN={}'.format(rasns, sasn))
            if sasn not in rasns:
                return sasn, utype + SINGLE_SUCC_RASN
    votes = succs + iasns
    log.debug('Votes: {}'.format(votes))
    if len(succs) > 1:
        if not any(iasn in succs for iasn in iasns):
            for iasn in iasns:
                if all(bdrmapit.bgp.peer_rel(iasn, sasn) for sasn in succs):
                    if votes[iasn] > max(votes.values()) / 2:
                        return iasn, utype + ALLPEER_SUCC
        iasn_in_succs = [iasn for iasn in iasns if iasn in succs]
        if len(iasn_in_succs) == 1:
            iasn = iasn_in_succs[0]
            if all(bdrmapit.bgp.peer_rel(iasn, sasn) or bdrmapit.bgp.provider_rel(sasn, iasn) for sasn in succs if sasn != iasn):
                if votes[iasn] > max(votes.values()) / 2:
                    return iasn, IASN_SUCC_HALF
    if len(succs) == 1 and len(iasns) > 1 and not any(iasn in succs for iasn in iasns):
        for sasn in succs:
            if all(bdrmapit.bgp.peer_rel(iasn, sasn) for iasn in iasns):
                return sasn, utype + ALLPEER_ORIGIN
    if not votes:
        return -1, -1
    allorigins = {o for os in succ_origins.values() for o in os}
    remaining = succs.keys() - allorigins
    log.debug('AllOrigins={}, Remaining={}'.format(allorigins, remaining))
    if len(remaining) == 1:
        asn = peek(remaining)
        if any(bdrmapit.bgp.customer_rel(asn, iasn) for iasn in allorigins):
            num = votes[asn]
            log.debug('Votes test: num={} > sum(votes)={}'.format(num, sum(votes.values())))
            if bdrmapit.bgp.conesize[asn] <= 0 and num >= (max(votes.values())) / 2:
                return asn, utype + REMAINING_4
    votes_rels = [vasn for vasn in votes if vasn in iasns or any(bdrmapit.bgp.rel(iasn, vasn) for iasn in iasns)]
    log.debug('Vote Rels: {}'.format(votes_rels))
    if len(votes_rels) < 2:
        votes_rels = votes
    else:
        for vasn in list(votes):
            if vasn not in votes_rels:
                for vr in votes_rels:
                    if bdrmapit.as2org[vr] == bdrmapit.as2org[vasn]:
                        votes[vr] += votes.pop(vasn, 0)
    asns = max_num(votes_rels, key=votes.__getitem__)
    othermax = max(votes, key=votes.__getitem__)
    if rtype != 3 and votes[othermax] > votes[asns[0]] * 4:
        utype += 3000
        return othermax, utype
    if len(asns) == 1:
        asn = asns[0]
        utype += VOTE_SINGLE
    else:
        asn = min(asns, key=lambda x: (bdrmapit.bgp.conesize[x], -x))
        utype += VOTE_TIE
    if asn not in iasns and not any(bdrmapit.bgp.rel(iasn, asn) for iasn in iasns):
        return hidden_asn(bdrmapit, iasns, asn, utype)
    return asn, utype


def annotate_routers(bdrmapit: Bdrmapit, rupdates: Updates, iupdates: Updates, routers: List[Router] = None, increment=100000):
    new_updates: Updates = rupdates.copy()
    if routers is None:
        routers = bdrmapit.graph.routers_succ
    pb = Progress(len(routers), 'Annotating routers', increment=increment)
    for router in pb.iterator(routers):
        asn, utype = annotate_router(bdrmapit, router, rupdates, iupdates)
        new_updates.add_update(router, asn, bdrmapit.as2org[asn], utype)
    return new_updates


def annotate_interfaces(bdrmapit: Bdrmapit, rupdates: Updates, iupdates: Updates, interfaces: List[Interface] = None):
    if interfaces is None:
        interfaces = bdrmapit.graph.interfaces_pred
    new_updates: Updates = iupdates.copy()
    pb = Progress(len(interfaces), 'Adding links', increment=200000)
    for interface in pb.iterator(interfaces):
        if interface.asn >= 0:
            asn, utype = annotate_interface(bdrmapit, interface, rupdates)
            new_updates.add_update(interface, asn, bdrmapit.as2org[asn], utype)
    return new_updates


def annotate_interface(bdrmapit: Bdrmapit, interface, rupdates: Updates):
    edges = set(bdrmapit.graph.inexthop.get(interface))
    votes = Counter()
    for ipred in edges:
        rpred = bdrmapit.graph.interface_router[ipred]
        asn, _, _ = rupdates[rpred]
        log.debug('Addr={}, Router={}, ASN={}, RASN={}'.format(ipred.address, rpred.name, ipred.asn, asn))
        prefix, _, num = interface.address.rpartition('.')
        iprefix, _, inum = ipred.address.rpartition('.')
        same = False
        if prefix == iprefix:
            if -1 <= int(num) - int(inum) <= 1:
                log.debug('Prefix={}, Diff={}, Same={}'.format(prefix, int(num) - int(inum), same))
                same = True
        if not same and interface.org == ipred.org:
            asn = ipred.asn
        else:
            log.debug('Router={}, RASN={}'.format(rpred.name, asn))
            if asn == -1:
                asn = ipred.asn
        votes[asn] += 1
    log.debug('Votes: {}'.format(votes))
    if len(votes) == 1:
        return peek(votes), 1 if len(edges) > 1 else 0
    asns = max_num(votes, key=votes.__getitem__)
    asn = max(asns, key=lambda x: (x == interface.asn, bdrmapit.bgp.conesize[x], -x))
    utype = 1 if len(asns) == 1 and len(edges) > 1 else 2
    return asn, utype


def get(bdrmapit, r: Router, updates: Updates):
    result = bdrmapit.lhupdates[r]
    if result[0] == -1:
        return updates[r]
    return result


def get_edges(bdrmapit: Bdrmapit, router):
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
    return set(edges), rtype


def get_origins(bdrmapit: Bdrmapit, router: Router, interface: Interface, rtype):
    if rtype == 1:
        return bdrmapit.graph.rnh_ases[router, interface]
    elif rtype == 2:
        return bdrmapit.graph.re_ases[router, interface]
    else:
        return bdrmapit.graph.rm_ases[router, interface]