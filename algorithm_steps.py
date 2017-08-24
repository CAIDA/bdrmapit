from collections import defaultdict, Counter, namedtuple
from logging import getLogger

import numpy as np

from create_objs_container import NodesContainerDict
from edge import Priority, Type
from utils.progress import Progress
from updates_dict import Updates
from utils.utils import max_num

log = getLogger()
bgp = None
allpaths = None
paths = None
utype = None
tparty = None
same = None
use_otherside = False
update_all = False
check_done = False
as2org = None
follow = None
iteration = 0

ECHO = 1
IXP = 2
UNANNOUNCED = 3
SAME = 4
THIRD_PARTY = 5
ANNOTATION = 6

DEBUG = False


def select_most_frequent(orgs):
    most_frequent = max_num(orgs, key=lambda x: sum(orgs[x].values()))
    if len(most_frequent) > 1:
        return router_tiebreaker2({o: orgs[o] for o in most_frequent})
    org = most_frequent[0]
    asns = orgs[org]
    asn = max(asns, key=lambda x: asns[x])
    return asn, org


def router_tiebreaker2(orgs):
    asns = {max(c, key=bgp.conesize): org for org, c in orgs.items()}
    min_asns = max_num(asns, key=lambda x: -bgp.conesize(x))
    asn = min(min_asns)
    return asn, asns[asn]


def election(router, updates):
    succ_type = router.succ_type
    orgs = None
    while succ_type <= Priority.none and (not orgs or (len(orgs) == 1 and '0' in orgs)):
        if succ_type <= Priority.multi:
            orgs = as_frequency_succ(router, succ_type, updates)
            # print(orgs)
        # elif succ_type < Priority.none:
        #     orgs = as_frequency_multi(router, succ_type, updates)
        else:
            orgs = defaultdict(Counter)
        succ_type += 1
    # print(orgs)
    seen = set()
    router_orgs = defaultdict(Counter)
    for interface in router.interfaces:
        if interface.succ_type < succ_type:
            iasn, iorg = otherside_mapping(interface, updates)
            if iasn > 0 and iorg not in seen:
                router_orgs[iorg][iasn] += 1
                seen.add(iorg)
                orgs[iorg][iasn] += 1
    # print(orgs)
    if len(orgs) == 1:
        for org, asns in orgs.items():
            return max(asns, key=lambda x: asns[x]), org
    elif orgs:
        most_frequent = max_num(orgs, key=lambda x: sum(orgs[x].values()))
        if len(most_frequent) == 1:
            org = most_frequent[0]
            asns = orgs[org]
            return max(asns, key=lambda x: asns[x]), org
        else:
            subsequent_orgs = set(orgs.keys()) - set(router_orgs.keys())
            if len(subsequent_orgs) == 1:
                for org in subsequent_orgs:
                    asns = orgs[org]
                    asn = max(asns, key=lambda x: asns[x])
                    if len(router_orgs) > 1:
                        return asn, org
                    for rorg, rasns in router_orgs.items():
                        rasn = max(rasns, key=lambda x: rasns[x])
                        if rasn in bgp.peer[asn]:
                            return asn, org
                        return router_tiebreaker2(orgs)
            # return router_tiebreaker2(orgs)
            interface_orgs = defaultdict(Counter)
            for interface in router.interfaces:
                if interface.succ_type < succ_type:
                    interface_orgs[interface.org][interface.asn] += 1
            most_frequent = max_num(interface_orgs, key=lambda x: sum(orgs[x].values()))
            if len(most_frequent) == 1:
                org = most_frequent[0]
                asns = interface_orgs[org]
                return max(asns, key=lambda x: asns[x]), org
            return router_tiebreaker2(interface_orgs)
    return router.asn, router.org


def missing_provider(asns, sasn):
    relationships = [asn for asn in asns if (asn, sasn) in bgp.relationships]
    if len(relationships) > 1 or len(relationships) == len(asns):
        return sasn
    elif len(relationships) == 1:
        return relationships[0]
    else:
        providers = bgp.providers[sasn]
        orgs = {as2org[asn] for asn in asns}
        if any(as2org[asn] in orgs for asn in providers):
            return None
        mutual = Counter()
        for asn in asns:
            mutual.update(bgp.customers[asn] & providers)
        missing = max_num(mutual, key=lambda x: mutual[x])
        if len(missing) == 1:
            return missing[0]
    return None


def has_relationship(other_asn, asns, org=None):
    if other_asn > 0:
        if org is None:
            org = as2org[other_asn]
        for asn in asns:
            if asn > 0 and (org == as2org[asn] or (other_asn, asn) in bgp.relationships):
                return True
    return False


def immediate_heuristics(successor, priority, ptype, asns, updates):
    sasn = successor.asn
    sorg = successor.org
    srouter = successor.router
    if ptype == Type.echo:  # Echo
        rasn, rorg, _ = updates.get(srouter, (srouter.asn, srouter.org, -1))
        if has_relationship(rasn, asns, org=rorg):
            return rasn, ECHO, rasn
        result = missing_provider(asns, rasn)
        return result, ECHO, rasn
    if sasn == -1:  # IXP
        return max(asns, key=bgp.conesize), IXP, None
    if sasn == 0:  # Unannounced
        rasn, rorg, _ = updates.get(srouter, (srouter.asn, srouter.org, -1))
        # print(rasn, rorg, srouter.id)
        if rasn == 0 or has_relationship(rasn, asns, org=rorg):
            return rasn, UNANNOUNCED, rasn
        result = missing_provider(asns, rasn)
        return result, UNANNOUNCED, rasn
    # Same
    for asn in asns:
        if as2org[asn] == sorg:
            return sasn, SAME, sasn
    rasn, rorg, _ = updates.get(srouter, (srouter.asn, srouter.org, -1))
    # print(sasn, rasn, asns)
    if sorg != rorg and has_relationship(rasn, asns, org=rorg):  # Third Party
        if not has_relationship(sasn, asns, org=sorg):  # Definite Third Party
            return rasn, THIRD_PARTY, rasn
        return None, THIRD_PARTY, rasn
    # Current Interface Annotation
    aasn, aorg, around = updates.get(successor, (sasn, sorg, -1))
    if True or priority != Priority.multi:
        if has_relationship(aasn, asns, org=aorg):
            return aasn, ANNOTATION, aasn
        result = missing_provider(asns, aasn)
        return result, ANNOTATION, aasn
    return aasn, ANNOTATION, aasn  # Use annotation


SuccRow = namedtuple('SuccRow', ['addr', 'asn', 'uasn', 'rasn', 'urasn', 'priority', 'type'])


def as_frequency_succ(router, succ_type, updates):
    orgs = defaultdict(Counter)
    succ = router.succ
    tps = []
    missing = []
    for (successor, priority, ptype), asns in succ.items():
        if priority == succ_type:
            label, reason, default = immediate_heuristics(successor, priority, ptype, asns, updates)
            if DEBUG:
                srow = SuccRow(successor.address, successor.asn, updates.asn_default(successor), successor.router.asn, updates.asn_default(successor.router), priority, ptype)
                print('{}: {} {} {}'.format(srow, label, reason, default))
            if label is not None:
                if label > 0:
                    org = as2org[label]
                    orgs[org][label] += 1
            elif reason == THIRD_PARTY:
                tps.append(((successor, priority, ptype), asns))
            else:
                missing.append(((successor, priority, ptype), asns, default))
    if DEBUG:
        print(orgs)
        print(tps)
        print(missing)
    resolve_third_parties(tps, orgs, router)
    resolve_missing(missing, orgs, router)
    if DEBUG:
        print(orgs)
    return orgs


def resolve_third_parties(tps, orgs, router):
    for _, asns in tps:
        router_allasns = paths.allasns[router]
        if sum(1 for asn in router_allasns if asn not in asns) <= 1:
            for _, asn in router_allasns.items():
                orgs[as2org[asn]][asn] += 1
        else:
            asn = max(asns, key=lambda x: asns[x])
            orgs[as2org[asn]][asn] += 1


def resolve_missing(missing, orgs, router):
    for _, asns, default in missing:
        orgs[as2org[default]][default] += 1


def otherside_mapping(interface, updates):
    if use_otherside:
        otherside = interface.otherside
        if otherside:
            asn, org, _ = updates.get(otherside, (interface.asn, interface.org, -1))
            return asn, org
    return interface.asn, interface.org


def connected_org_router(router, updates):
    return election(router, updates)


def otherside_router_mapping(interface, updates):
    if use_otherside:
        otherside = interface.otherside
        if otherside:
            orouter = otherside.router
            asn, org, _ = updates.get(orouter, (orouter.asn, orouter.org, -1))
            # print(asn, org)
            return asn, org
    return interface.asn, interface.org


PredRow = namedtuple('PredRow', ['addr', 'rid', 'asn', 'rasn', 'urasn'])


def interface_multi(interface, updates):
    orgs = defaultdict(Counter)
    if interface.pred_type == Priority.next_hop:
        for predecessor, priority, ptype in interface.pred:
            if priority == Priority.next_hop:
                prouter = predecessor.router
                rasn, rorg, _ = updates.get(prouter, (prouter.asn, prouter.org, -1))
                asn, org = pred_heuristics(interface, predecessor, rasn, rorg)
                if DEBUG:
                    prow = PredRow(predecessor.address, prouter.id, predecessor.asn, prouter.asn, rasn)
                    print('{}: {} {}'.format(prow, asn, org))
                orgs[org][asn] += 1
        if DEBUG:
            print(orgs)
        org = max_num(orgs, key=lambda x: sum(orgs[x].values()))
        if len(org) == 1:
            org = org[0]
            asns = orgs[org]
            asn = max(asns, key=lambda x: asns[x])
            if asn > 0:
                return asn, org
    iasn, iorg = otherside_router_mapping(interface, updates)
    return iasn, iorg


def pred_heuristics(interface, predecessor, rasn, rorg):
    # Unannounced
    if interface.asn == 0:
        return 0, '0'
    if predecessor.org == rorg:
        return rasn, rorg
    if interface.org == predecessor.org:
        return interface.asn, interface.org
    return rasn, rorg


def connected_org_interface(interface, updates):
    router = interface.router
    rasn, rorg, _ = updates.get(router, (router.asn, router.org, -1))
    if interface.org != rorg:
        return interface.asn, interface.org
    return interface_multi(interface, updates)


def annotate_routers(routers, updates, **kargs):
    set_global(**kargs)
    new_updates = updates.copy()
    pb = Progress(len(routers), 'Annotating routers', increment=50000, callback=lambda: 'Updates {:,d}'.format(len(new_updates)))
    for router in pb.iterator(routers):
        uround = updates.get(router, (None, None, -1))[-1]
        if uround == utype or uround == -1:
            network = connected_org_router(router, updates)
            asn, org = network
            new_updates.add_update(router, asn, org, utype=utype)
    return new_updates


def infer_links(interfaces, updates, **kargs):
    set_global(**kargs)
    new_updates = updates.copy()
    pb = Progress(len(interfaces), 'Adding links', increment=200000, callback=lambda: 'Updates {:,d}'.format(len(new_updates)))
    for interface in pb.iterator(interfaces):
        if interface.pred_type == Priority.next_hop:
            if interface.asn > 0:
                uround = updates.get(interface, (None, None, -1))[-1]
                if uround == utype or uround == -1:
                    network = connected_org_interface(interface, updates)
                    if network:
                        asn, org = network
                        new_updates.add_update(interface, asn, org, utype)
                    elif update_all:
                        new_updates.pop(interface, None)
    return new_updates


def set_global(**kargs):
    global bgp, allpaths, paths, utype, tparty, same, use_otherside, update_all, check_done, as2org, follow, iteration, DEBUG
    if kargs:
        allpaths = kargs.get('allpaths', allpaths)
        paths = kargs.get('paths', paths)
        bgp = kargs.get('bgp', bgp)
        utype = kargs.get('utype', utype)
        tparty = kargs.get('tparty', tparty)
        same = kargs.get('same', same)
        use_otherside = kargs.get('use_otherside', use_otherside)
        update_all = kargs.get('update_all', update_all)
        check_done = kargs.get('check_done', check_done)
        as2org = kargs.get('as2org', as2org)
        follow = kargs.get('follow', follow)
        iteration = kargs.get('iteration', 0)
        DEBUG = kargs.get('DEBUG', DEBUG)


def gateway_tests(router, orgs):
    asns = {asn for _, asn in orgs.items()}
    relations = {asn for asn in asns if all(interface.asn == asn or (interface.asn, asn) in bgp.relationships for interface in router.interfaces)}
    if len(relations) > 1:
        # Probably internal:
        return None
    if len(relations) == 1:
        # Choose the single asn
        for asn in relations:
            return asn, as2org[asn]
    # Check for a missing customer of the interfaces, provider of the ases
    missing_middle = defaultdict(set)
    for interface in router.interfaces:
        customers = bgp.customers[interface.asn]
        for asn in asns:
            for middle in customers & bgp.providers[asn]:
                missing_middle[middle].add(interface.asn)
    missing_customers = max_num(missing_middle, key=lambda x: len(missing_middle[x]))
    if len(missing_customers) == 1:
        # Choose the lone middle
        asn = missing_customers[0]
        return asn, as2org[asn]
    if len(asns) == 1:
        for asn in asns:
            return asn, as2org[asn]


def all_unreliable(router):
    if router.succ_type == Priority.unreliable:
        return all(priority == Priority.unreliable and ptype == Type.unreachable for _, _, priority, ptype in router.succ)
    return False


def gateway_heuristic(objs, updates, **kargs):
    set_global(**kargs)
    interface_asns = paths.interface_asns
    new_updates = updates.copy()
    pb = Progress(len(objs.routers), message='Identifying firewall routers', increment=1000000, callback=lambda: 'Updates {:,d}'.format(len(new_updates)))
    for router in pb.iterator(objs.routers.values()):
        if router.succ_type == Priority.none:
            skip = False
            orgs = {}
            for interface in router.interfaces:
                iorgs = interface_asns[interface]
                for org, asn in iorgs.items():
                    if org == interface.org:
                        skip = True
                        continue
                    orgs[org] = asn
            if orgs:
                network = gateway_tests(router, orgs)
                if network:
                    asn, org = network
                    new_updates.add_update(router, asn, org, utype=utype)
    return new_updates


def interfaces_only(router):
    if len(router.interfaces) == 1:
        for interface in router.interfaces:
            if interface.asn != -1:
                return interface.asn, interface.org
            else:
                return 0, '0'
    else:
        orgs = defaultdict(Counter)
        for interface in router.interfaces:
            if interface.asn != -1:
                orgs[interface.org][interface.asn] += 1
        if orgs:
            org = max_num(orgs, key=lambda x: sum(orgs[x].values()))
            if len(org) == 1:
                org = org[0]
                asns = orgs[org]
                asn = max(asns, key=lambda x: asns[x])
                return asn, org
            else:
                return router_tiebreaker2({o: orgs[o] for o in org})
        else:
            return 0, '0'


def annotate_initial(objs, routers=None, **kargs):
    set_global(**kargs)
    nonzero = 0
    if isinstance(objs, NodesContainerDict):
        num_routers = objs.num_routers
        routers = objs.routers
    else:
        num_routers = len(objs.routers)
        routers = objs.routers.values()
    pb = Progress(num_routers, 'Annotating routers initially', increment=1000000, callback=lambda: 'Nonzero {:,d}'.format(nonzero))
    for router in pb.iterator(routers):
        asn, org = interfaces_only(router)
        router.asn = asn
        router.org = org
        if asn > 0:
            nonzero += 1


def finish_links(interfaces, updates, **kargs):
    set_global(**kargs)
    changed = 0
    new_updates = updates.copy()
    pb = Progress(len(interfaces), 'Finishing links', increment=500000, callback=lambda: '{:,d}'.format(changed))
    for interface in pb.iterator(interfaces):
        if interface.asn > 0:
            if interface.pred_type == Priority.next_hop:
                if sum(1 for _, priority, _ in interface.pred if priority == Priority.next_hop) == 1:
                    for predecessor, priority, ptype in interface.pred:
                        if priority == Priority.next_hop:
                            if interface.org != predecessor.org:
                                prouter = predecessor.router
                                pasn, porg, pround = updates.get(prouter, (prouter.asn, prouter.org, -1))
                                if interface.org != porg:
                                    if (interface.asn, predecessor.asn) in bgp.relationships:
                                        asn, org = predecessor.asn, predecessor.org
                                    else:
                                        asn, org = pasn, porg
                                    new_updates.add_update(interface, asn, org, utype)
                                    changed += 1
    return new_updates


def print_accuracy(verify, updates, interfaces):
    v = verify.verify(updates, **interfaces)
    log.info(str(v))


def algorithm(objs, updates=None, previous_updates=None, iterations=np.PINF, verify=None, routers=None, interfaces=None, verify_interfaces=None, routers_only=False, **kargs):
    set_global(**kargs)
    global paths, iteration
    if routers is None:
        routers = [router for router in objs.routers.values() if router.succ]
    if interfaces is None:
        interfaces = [interface for interface in objs.interfaces.values() if interface.pred_type == Priority.next_hop]
    if updates is None:
        updates = Updates()
    if verify_interfaces is None and verify is not None:
        raise Exception('Must supply verify interfaces')
    print_accuracy(verify, updates, verify_interfaces)
    while iteration < iterations:
        log.info('***** Iteration {} *****'.format(iteration))
        updates = annotate_routers(routers, updates)
        print_accuracy(verify, updates, verify_interfaces)
        if routers_only:
            return updates
        updates = infer_links(interfaces, updates)
        print_accuracy(verify, updates, verify_interfaces)
        if check_done:
            if iteration < iterations - 1 and updates in previous_updates:
                break
            if isinstance(previous_updates, list):
                previous_updates.append(updates)
        iteration += 1
    return updates
