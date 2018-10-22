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
                    if name is not None and name.text == 'inet' or name.text == 'inet6':
                        for local in family.findall('.//{*}ifa-local'):
                            addr = local.text
                            info = Info(addr, rname, pdesc, ldesc)
                            nodes[router].append(info)
                            addrs[addr] = info
    return nodes, addrs


def verify(pairs, i2addrs, addrs, prior, after):
    marked = {x for x, _ in pairs}
    tps = set()
    tns = set()
    fps = set()
    fns = set()
    mfns = set()
    for a in i2addrs.keys() & addrs:
        info = i2addrs[a]
        if info.ldesc:
            ldesc = info.ldesc.lower()
            if 'loopback' in info.ldesc.lower():
                continue
            vrf = 'trcps' in ldesc or 'transitrail' in ldesc
            if a in ['64.57.22.235', '64.57.22.227']:
                vrf = True
            p = prior[a]
            if vrf:
                if a in marked:
                    tps.add(a)
                elif 11537 not in p and 11164 not in p:
                    tns.add(a)
                else:
                    fns.add(a)
                    if after[a]:
                        mfns.add(a)
            else:
                if a in marked:
                    fps.add(a)
                else:
                    tns.add(a)
    tp = len(tps)
    tn = len(tns)
    fp = len(fps)
    fn = len(fns)
    mfn = len(mfns)
    try:
        ppv = (tp / (tp + fp))
    except ZeroDivisionError:
        ppv = 0
    try:
        recall = (tp / (tp + fn))
    except ZeroDivisionError:
        recall = 0
    try:
        mrecall = (tp / (tp + mfn))
    except ZeroDivisionError:
        mrecall = 0
    print('TP {:,d} FP {:,d} FN {:,d} TN {:,d} PPV {:.1%} Recall {:.1%}'.format(tp, fp, fn, tn, ppv, recall))
    print('MFN {:,d} MRecall {:.1%}'.format(mfn, mrecall))
    return tps, tns, fps, fns, mfns
