import json
import os
import socket
import sqlite3
from collections import Counter
from typing import Set, Tuple, Dict, Iterable

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
        for i in range(numhops - 1):
            x = trace.hops[i]
            y = trace.hops[i+1]
            z = trace.hops[i+2] if i < numhops - 2 else None
            distance = self.compute_dist(x, y, z)
            if y.addr in marked:
                if x.asn in basns[y.addr]:
                    self.adjs.add((x.addr, y.addr, distance, y.icmp_type, BFORWARD))
                elif z:
                    if (y.addr, z.addr) not in pairs:
                        if z.asn in aasns[y.addr]:
                            self.adjs.add((x.addr, y.addr, distance, y.icmp_type, BFORWARD))
                    if i < numhops - 3:
                        zz = trace.hops[i+3]
                        if zz.asn in aasns[y.addr]:
                            self.adjs.add((x.addr, y.addr, distance, y.icmp_type, BFORWARD))
            elif x.addr in marked:
                if (x.addr, y.addr) in pairs:
                    self.adjs.add((x.addr, y.addr, 1, 11, BBACKWARD))
                else:
                    if i > 0:
                        w = trace.hops[i-1]
                        if w.asn in basns[x.addr]:
                            self.adjs.add((x.addr, y.addr, distance, y.icmp_type, BBACKWARD))
                    if y.asn in aasns[x.addr]:
                        self.adjs.add((x.addr, y.addr, distance, y.icmp_type, DOUBLE))
            else:
                self.adjs.add((x.addr, y.addr, distance, y.icmp_type, NONE))
            self.dists[(x.addr, y.addr)] += 1 if distance == 1 else -1

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
