import json
import socket
import sqlite3
import sys
from collections import namedtuple, defaultdict
from multiprocessing.pool import Pool

from lxml import etree
from typing import List, Set, Dict

from lxml.etree import _Element

from bgp.routing_table import RoutingTable
from traceroute.warts import Warts
from traceroute.warts_trace import WartsTrace
from utils.progress import Progress

Info = namedtuple('Info', ['addr', 'router', 'pdesc', 'ldesc'])
ip2as: RoutingTable = None
addrs: Set[str] = None


def parse_xml(filename):
    nodes = defaultdict(list)
    addrs = {}
    parser = etree.XMLParser(ns_clean=True, recover=True)
    root = etree.parse(filename, parser)
    routers: List[_Element] = root.findall('.//{*}router')
    for router in routers:
        rname = router.attrib['name']
        iface_info = router.find('{*}interface-information')
        for physical in iface_info.findall('.//{*}physical-interface'):
            pdesc = physical.find('.//{*}description')
            pdesc = pdesc.text if pdesc is not None else None
            for logical in physical.findall('.//{*}logical-interface'):
                ldesc = logical.find('.//{*}description')
                ldesc = ldesc.text if ldesc is not None else None
                for family in logical.findall('.//{*}address-family'):
                    name = family.find('.//{*}address-family-name')
                    if name is not None and name.text == 'inet':
                        for local in family.findall('.//{*}ifa-local'):
                            addr = local.text
                            info = Info(addr, rname, pdesc, ldesc)
                            nodes[router].append(info)
                            addrs[addr] = info
    return nodes, addrs


def family(a):
    if '.' in a:
        return socket.AF_INET
    else:
        return socket.AF_INET6


def pton(addr, fam=None):
    if fam is None:
        fam = family(addr)
    return int.from_bytes(socket.inet_pton(fam, addr), 'big')


def ntop(num: int, fam):
    length = 4 if fam == socket.AF_INET else 16
    return socket.inet_ntop(fam, num.to_bytes(length, 'big'))


def subnet31(x, y):
    x = pton(x)
    y = pton(y)
    if x < y:
        x, y = y, x
    if x % 2 == 1:
        return True
    return False


def otherside31(x):
    fam = family(x)
    x = pton(x, fam=fam)
    if x % 2 == 1:
        oside = x - 1
    else:
        oside = x + 1
    return ntop(oside, fam)


def extract_pairs(filename, **kwargs):
    for k, v in kwargs.items():
        globals()[k] = v
    pairs = set()
    marked = set()
    baddrs = defaultdict(set)
    aaddrs = defaultdict(set)
    basns = defaultdict(set)
    aasns = defaultdict(set)
    aspaths = set()
    with Warts(filename, json=True) as f:
        pb = Progress(message='Reading', increment=100000, callback=lambda: 'Pairs {:,d}'.format(len(pairs)))
        for j in pb.iterator(f):
            trace = WartsTrace(j, ip2as=ip2as)
            for i in range(len(trace.hops) - 1):
                x = trace.hops[i]
                y = trace.hops[i+1]
                w = trace.hops[i-1] if i > 0 else None
                z = trace.hops[i+2] if i < len(trace.hops) - 2 else None
                # if z and x.addr == '198.71.47.201' and z.addr == '72.195.173.174':
                #     print(json.dumps(j), file=sys.stderr)
                if x.addr == '162.252.70.82' and y.addr == '162.252.70.83':
                    print(json.dumps(j), file=sys.stderr)
                xnum = pton(x.addr)
                ynum = pton(y.addr)
                if abs(xnum - ynum) == 1 or (-100 >= x.asn == y.asn):
                    pairs.add((x.addr, y.addr))
                    marked.add(x.addr)
                    if w and w.addr != x.addr:
                        if w.asn > 0:
                            baddrs[x.addr, y.addr].add(w.addr)
                            basns[x.addr, y.addr].add(w.asn)
                    if z and z.addr != y.addr:
                        z = trace.hops[i+2]
                        if z.asn > 0:
                            aaddrs[x.addr, y.addr].add(z.addr)
                            aasns[x.addr, y.addr].add(z.asn)
                # if w and z:
                #     if w.asn > 0 and z.asn > 0:
                #         if w.addr != x.addr and x.addr != y.addr:
                #             aspaths.add((x.addr, w.asn, z.asn))
        return pairs, baddrs, aaddrs, basns, aasns, aspaths


def addasns(basns, aasns, aspaths):
    osums = (sum(len(v) for v in basns), sum(len(v) for v in aasns))
    sums = None
    while sums != osums:
        print('Again!')
        for addr, paths in aspaths.items():
            for x, y in paths:
                if x in basns[addr]:
                    aasns[addr].add(y)
                if y in aasns[addr]:
                    basns[addr].add(x)
        osums, sums = sums, (sum(len(v) for v in basns), sum(len(v) for v in aasns))


def run(filenames: List[str], ip2as: RoutingTable, addrs: Set[str], poolsize=40):
    globals()['ip2as'] = ip2as
    globals()['addrs'] = addrs
    pairs = set()
    baddrs = defaultdict(set)
    aaddrs = defaultdict(set)
    basns = defaultdict(set)
    aasns = defaultdict(set)
    aspaths = set()
    Progress.set_output(True)
    pb = Progress(len(filenames), 'Processing traceroutes', callback=lambda: ' Pairs {:,d} BAddrs {:,d} AAddrs {:,d} ASPaths {:,d}'.format(len(pairs), len(baddrs), len(aaddrs), len(aspaths)))
    Progress.set_output(False)
    with Pool(poolsize) as pool:
        for newpairs, newbasns, newaasns in pb.iterator(pool.imap_unordered(extract_pairs, filenames)):
        # for newpairs, newbaddrs, newaaddrs, newbasns, newaasns, newaspaths in pb.iterator(pool.imap_unordered(extract_pairs, filenames)):
            pairs.update(newpairs)
            # for k, v in newbaddrs.items():
            #     baddrs[k].update(v)
            # for k, v in newaaddrs.items():
            #     aaddrs[k].update(v)
            for k, v in newbasns.items():
                basns[k].update(v)
            for k, v in newaasns.items():
                aasns[k].update(v)
            # aspaths.update(newaspaths)
    Progress.set_output(True)
    return pairs, baddrs, aaddrs, basns, aasns


def remove_internal(pairs, basns, aasns, addrs, ip2as):
    pairs2 = set()
    ba = {}
    aa = {}
    pb = Progress(len(pairs), 'Checking', increment=100000,
                  callback=lambda: 'Pairs {:,d} BASNs {:,d} AASNs {:,d}'.format(len(pairs2), len(ba), len(aa)))
    for x, y in pb.iterator(pairs):
        osidex = otherside31(x)
        osidey = otherside31(y)
        if ip2as[x] < 0 or osidex == y or (osidex not in addrs and osidey not in addrs):
            if ip2as[x] not in basns[x, y] & aasns[x, y]:
                pairs2.add((x, y))
                ba[x, y] = basns[x, y]
                aa[x, y] = aasns[x, y]
    return pairs2, ba, aa
