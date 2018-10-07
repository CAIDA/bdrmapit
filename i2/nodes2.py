import socket
import sqlite3
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
addrs: Dict[str, Info] = None


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


# def extract_pairs(filename, **kwargs):
#     for k, v in kwargs.items():
#         globals()[k] = v
#     marked = set()
#     pairs = set()
#     con = sqlite3.connect(filename)
#     pb = Progress(message='Reading triplets', increment=1000000, callback=lambda: 'Marked {:,d} Pairs {:,d}'.format(len(marked), len(pairs)))
#     for x, y, z, plusone in pb.iterator(con.execute('SELECT hop1, hop2, hop3, plusone FROM adjacency WHERE distance = 1')):
#         if y in addrs:
#             yasn = ip2as[y]
#             zasn = ip2as[z]
#             if yasn <= -100 and yasn == zasn:
#                 plusone = True
#             if plusone:
#                 pairs.add((y, z))
#                 marked.add(y)
#     baddrs = defaultdict(set)
#     aaddrs = defaultdict(set)
#     pb = Progress(message='Reading triplets', increment=1000000)
#     for x, y, z in pb.iterator(con.execute('SELECT hop1, hop2, hop3 FROM adjacency')):
#         if x:
#             if x in marked:
#                 aaddrs[x].add(z)
#             if y in marked:
#                 baddrs[y].add(x)
#     basns = defaultdict(set)
#     aasns = defaultdict(set)
#     for addr in baddrs.keys() | aaddrs.keys():
#         for baddr in baddrs[addr]:
#             asn = ip2as[baddr]
#             if asn > 0:
#                 basns[addr].add(asn)
#         for aaddr in aaddrs[addr]:
#             asn = ip2as[aaddr]
#             if asn > 0:
#                 aasns[addr].add(asn)
#     con.close()
#     return basns, aasns, pairs


# def extract_pairs(filename, **kwargs):
#     for k, v in kwargs.items():
#         globals()[k] = v
#     marked = set()
#     pairs = set()
#     con = sqlite3.connect(filename)
#     pb = Progress(message='Reading triplets', increment=1000000, callback=lambda: 'Marked {:,d} Pairs {:,d}'.format(len(marked), len(pairs)))
#     for x, y, z, plusone in pb.iterator(con.execute('SELECT hop1, hop2, hop3, plusone FROM adjacency WHERE distance = 1')):
#         if y in addrs:
#             yasn = ip2as[y]
#             zasn = ip2as[z]
#             if yasn <= -100 and yasn == zasn:
#                 plusone = True
#             if plusone:
#                 pairs.add((y, z))
#                 marked.add(y)
#     baddrs = defaultdict(set)
#     aaddrs = defaultdict(set)
#     pb = Progress(message='Reading triplets', increment=1000000)
#     for x, y, z in pb.iterator(con.execute('SELECT hop1, hop2, hop3 FROM adjacency')):
#         if x:
#             if (x, y) in pairs:
#                 aaddrs[x].add(z)
#             if (y, z) in pairs:
#                 baddrs[y].add(x)
#     basns = defaultdict(set)
#     aasns = defaultdict(set)
#     for addr in baddrs.keys() | aaddrs.keys():
#         for baddr in baddrs[addr]:
#             asn = ip2as[baddr]
#             if asn > 0:
#                 basns[addr].add(asn)
#         for aaddr in aaddrs[addr]:
#             asn = ip2as[aaddr]
#             if asn > 0:
#                 aasns[addr].add(asn)
#     baddrs2 = defaultdict(set)
#     aaddrs2 = defaultdict(set)
#     pb = Progress(message='Reading triplets', increment=1000000)
#     for x, y, z in pb.iterator(con.execute('SELECT hop1, hop2, hop3 FROM adjacency')):
#         if x:
#             if x in marked:
#                 aaddrs2[x].add(z)
#             if (y, z) in pairs:
#                 baddrs2[y].add(x)
#     con.close()
#     return basns, aasns


def family(a):
    if '.' in a:
        return socket.AF_INET
    else:
        return socket.AF_INET6


def pton(addr):
    return int.from_bytes(socket.inet_pton(family(addr), addr), 'big')


def extract_pairs(filenames, **kwargs):
    for k, v in kwargs.items():
        globals()[k] = v
    if isinstance(filenames, str):
        filenames = [filenames]
    pairs = set()
    for filename in filenames:
        with Warts(filename, json=True) as f:
            pb = Progress(message='Reading', increment=100000, callback=lambda: 'Pairs {:,d}'.format(len(pairs)))
            for j in pb.iterator(f):
                trace = WartsTrace(j, ip2as=ip2as)
                for i in range(len(trace.hops) - 1):
                    x = trace.hops[i]
                    y = trace.hops[i+1]
                    xnum = pton(x.addr)
                    ynum = pton(y.addr)
                    if abs(xnum - ynum) == 1:
                        pairs.add((x.addr, y.addr))
    return pairs


def run(dbs: List[str], ip2as: RoutingTable, addrs: Dict[str, Info], poolsize=40):
    globals()['ip2as'] = ip2as
    globals()['addrs'] = addrs
    basns = defaultdict(set)
    aasns = defaultdict(set)
    pb = Progress(len(dbs), 'Processing triplets', callback=lambda: '{:,d}'.format(len(basns)))
    Progress.set_output(False)
    with Pool(poolsize) as pool:
        for newbasns, newaasns in pb.iterator(pool.imap_unordered(extract_pairs, dbs)):
            for addr, asns in newbasns.items():
                basns[addr].update(asns)
            for addr, asns in newaasns.items():
                aasns[addr].update(asns)
    Progress.set_output(True)
    return basns, aasns
