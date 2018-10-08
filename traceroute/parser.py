import json
import os
import socket
import sqlite3
from collections import Counter
from itertools import groupby
from operator import itemgetter
from typing import Set, Tuple, Dict, Iterable, List

from traceroute.atlas_trace import AtlasTrace
from traceroute.hop import Hop
from traceroute.output_type import OutputType
from traceroute.warts import Warts
from traceroute.warts_trace import WartsTrace
from utils.utils import File2


FORWARD = 1
BACKWARD = 2
BOTH = 3

NONE = 0
ADJEXCLUDE = 1

BFORWARD = 1
BBACKWARD = 2
DOUBLE = 3

ADDR = 'address'
ADJ = 'adjacency'
DP = 'destpair'
DIST = 'distance'

pairs: Set[Tuple[str, str]] = None
basns: Dict[str, Set[int]] = None
aasns: Dict[str, Set[int]] = None
marked: Set[str] = None


def unique_justseen(iterable, key=None):
    "List unique elements, preserving order. Remember only the element just seen."
    # unique_justseen('AAAABBBCCDAABBB') --> A B C D A B
    # unique_justseen('ABBCcAD', str.lower) --> A B C A D
    return map(next, map(itemgetter(1), groupby(iterable, key)))


def opendb(filename, remove=False):
    if remove:
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
    try:
        con = sqlite3.connect(filename)
    except sqlite3.OperationalError:
        print(filename)
        raise
    cur = con.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS address (addr TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS adjacency (hop1 TEXT, hop2 TEXT, distance INT, type INT, special INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS destpair (addr TEXT, asn INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS distance (hop1 TEXT, hop2 TEXT, distance INT)')
    cur.close()
    con.commit()
    return con


class Parser:

    def __init__(self, filename, output_type, ip2as):
        self.filename = filename
        self.output_type = output_type
        self.ip2as = ip2as
        self.addrs = set()
        self.adjs = set()
        self.dps = set()
        self.dists = Counter()
        self.trips = set()

    def __iter__(self):
        if isinstance(self.filename, str):
            if self.output_type == OutputType.warts:
                with Warts(self.filename, json=True) as f:
                    for j in f:
                        if j['type'] == 'trace':
                            yield WartsTrace(j, ip2as=self.ip2as)
            elif self.output_type == OutputType.atlas:
                with File2(self.filename) as f:
                    for j in map(json.loads, f):
                        yield AtlasTrace(j, ip2as=self.ip2as)
        else:
            for j in self.filename:
                yield WartsTrace(j, ip2as=self.ip2as)

    def compute_dist(self, x: Hop, y: Hop, z: Hop = None):
        distance = y.ttl - x.ttl
        if y.qttl == 0:
            if z and (y.addr == z.addr or y.reply_ttl - z.reply_ttl == (z.ttl - y.ttl) - 1):
                distance -= y.qttl - x.qttl
        elif y.qttl > 1:
            if y.icmp_type == 3 and y.qttl - x.qttl >= y.ttl - x.ttl:
                distance -= y.qttl - x.qttl
        if distance > 1:
            distance = 2
        elif distance < 1:
            distance = -1
        return distance

    def parseone(self, trace):
        self.addrs.update((h.addr,) for h in trace.allhops if not h.private)
        numhops = len(trace.hops)
        if numhops == 0:
            return
        dest_asn = trace.dst_asn
        if dest_asn > 0:
            self.dps.update((y.addr, dest_asn) for y in trace.hops if y.icmp_type != 0)
        vrfs = set()
        for i in range(numhops):
            x = trace.hops[i]
            if x.addr in marked:
                y = trace.hops[i+1] if i < numhops - 1 else None
                w = None
                for j in range(i-1, -1, -1):
                    w = trace.hops[j]
                    if w.addr != x.addr:
                        break
                z = None
                for j in range(i+2, numhops):
                    z = trace.hops[j]
                    if z.addr != y.addr:
                        break
                if w and (w.addr, x.addr) in pairs:
                    continue
                if w and w.asn in aasns[x.addr]:
                    continue
                if z and z.asn in basns[x.addr]:
                    continue
                if (not w or (w.asn > 0 and w.asn not in basns[x.addr])) and (not z or (z.asn > 0 and z.asn in aasns[x.addr])):
                    continue
                vrfs.add(x.addr)
        for i in range(numhops - 1):
            x = trace.hops[i]
            y = trace.hops[i+1]
            z = trace.hops[i+2] if i < numhops - 2 else None
            distance = self.compute_dist(x, y, z)
            if x.addr in vrfs:
                if y.addr in vrfs:
                    special = DOUBLE
                else:
                    special = BBACKWARD
            elif y.addr in vrfs:
                special = BFORWARD
            else:
                special = NONE
            self.adjs.add((x.addr, y.addr, distance, y.icmp_type, special))
            self.dists[(x.addr, y.addr)] += 1 if distance == 1 else -1

    def find_trips(self, trace):
        # hops: List[Hop] = list(trace.unique_justseen())
        hops: List[Hop] = list(unique_justseen(trace.hops, key=lambda hop: hop.addr))
        numhops = len(hops)
        for i in range(1, numhops - 1):
            x = hops[i]
            if x.addr in marked:
                w = hops[i - 1]
                y = hops[i + 1]
                if (x.addr, y.addr) in pairs or (w.addr, x.addr) in pairs:
                    continue
                if w.asn == 0 or y.asn == 0:
                    continue
                self.trips.add((w.asn, x.addr, y.asn))

    def parse(self):
        for trace in self:
            self.parseone(trace)

    def reset(self):
        self.addrs = set()
        self.adjs = set()
        self.dps = set()
        self.dists = Counter()

    def to_sql(self, filename):
        con = opendb(filename, remove=True)
        insert_address(con, self.addrs)
        insert_adjacency(con, self.adjs)
        insert_destpair(con, self.dps)
        insert_distance(con, self.dists)
        con.close()


def insert_address(con: sqlite3.Connection, addrs: Set):
    cur = con.cursor()
    cur.executemany('INSERT INTO address (addr) VALUES (?)', addrs)
    cur.close()
    con.commit()


def insert_adjacency(con: sqlite3.Connection, adjs: Set):
    cur = con.cursor()
    cur.executemany('INSERT INTO adjacency (hop1, hop2, distance, type, special) VALUES (?, ?, ?, ?, ?)', adjs)
    cur.close()
    con.commit()


def insert_destpair(con: sqlite3.Connection, dps: Set):
    cur = con.cursor()
    cur.executemany('INSERT INTO destpair (addr, asn) VALUES (?, ?)', dps)
    cur.close()
    con.commit()


def insert_distance(con: sqlite3.Connection, dists: Counter):
    cur = con.cursor()
    cur.executemany('INSERT INTO distance (hop1, hop2, distance) VALUES (?, ?, ?)', ((x, y, n) for (x, y), n in dists.items()))
    cur.close()
    con.commit()


def family(a):
    if '.' in a:
        return socket.AF_INET
    else:
        return socket.AF_INET6


def addrdist(a1: str, a2: str):
    fam1 = family(a1)
    fam2 = family(a2)
    try:
        b1 = socket.inet_pton(fam1, a1)
        i1 = int.from_bytes(b1, 'big')
        b2 = socket.inet_pton(fam2, a2)
        i2 = int.from_bytes(b2, 'big')
    except OSError:
        print(a1, a2)
        raise
    return abs(i1-i2)
