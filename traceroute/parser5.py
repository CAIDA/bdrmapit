import json
import os
import socket
import sqlite3
from collections import Counter
from typing import Set

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

ADDR = 'address'
ADJ = 'adjacency'
DP = 'destpair'
DIST = 'distance'
TRIPS = 'triplets'


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
    cur.execute('CREATE TABLE IF NOT EXISTS address (addr TEXT, num INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS adjacency (hop1 TEXT, hop2 TEXT, hop3 TEXT, plusone BOOLEAN, distance INT, type INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS destpair (addr TEXT, asn INT, echo BOOLEAN)')
    cur.execute('CREATE TABLE IF NOT EXISTS distance (hop1 TEXT, hop2 TEXT, distance INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS triplets (hop1 TEXT, hop2 TEXT, hop3 TEXT)')
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
        if self.output_type == OutputType.warts:
            with Warts(self.filename, json=True) as f:
                for j in f:
                    if j['type'] == 'trace':
                        yield WartsTrace(j, ip2as=self.ip2as)
        elif self.output_type == OutputType.atlas:
            with File2(self.filename) as f:
                for j in map(json.loads, f):
                    yield AtlasTrace(j, ip2as=self.ip2as)

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
        self.addrs.update((h.addr, 0) for h in trace.allhops if not h.private)
        dest_asn = trace.dst_asn
        numhops = len(trace.hops)
        if numhops == 0:
            return
        i = 0
        y = trace.hops[0]
        if not y.private:
            self.dps.add((y.addr, dest_asn, y.icmp_type == 0))
        while i < numhops - 1:
            i += 1
            x: Hop = y
            y: Hop = trace.hops[i]
            z: Hop = trace.hops[i + 1] if i < numhops - 1 else None
            distance = self.compute_dist(x, y, z)
            self.adjs.add((x.addr, y.addr, distance, y.icmp_type))
            self.dps.add((y.addr, dest_asn, y.icmp_type == 0))
            self.dists[(x.addr, y.addr)] += 1 if distance == 1 else -1
            if z and addrdist(y.addr, z.addr) == 1:
                self.trips.add((x.addr, y.addr, z.addr))

    def parse(self):
        for trace in self:
            self.parseone(trace)

    def reset(self):
        self.addrs = set()
        self.adjs = set()
        self.dps = set()
        self.dists = Counter()
        self.trips = set()

    def to_sql(self, filename):
        con = opendb(filename, remove=True)
        insert_address(con, self.addrs)
        insert_adjacency(con, self.adjs)
        insert_destpair(con, self.dps)
        insert_distance(con, self.dists)
        insert_triplets(con, self.trips)
        con.close()


def insert_address(con: sqlite3.Connection, addrs: Set):
    cur = con.cursor()
    cur.executemany('INSERT INTO address (addr, num) VALUES (?, ?)', addrs)
    cur.close()
    con.commit()


def insert_adjacency(con: sqlite3.Connection, adjs: Set):
    cur = con.cursor()
    cur.executemany('INSERT INTO adjacency (hop1, hop2, distance, type) VALUES (?, ?, ?, ?)', adjs)
    cur.close()
    con.commit()


def insert_destpair(con: sqlite3.Connection, dps: Set):
    cur = con.cursor()
    cur.executemany('INSERT INTO destpair (addr, asn, echo) VALUES (?, ?, ?)', dps)
    cur.close()
    con.commit()


def insert_distance(con: sqlite3.Connection, dists: Counter):
    cur = con.cursor()
    cur.executemany('INSERT INTO distance (hop1, hop2, distance) VALUES (?, ?, ?)', ((x, y, n) for (x, y), n in dists.items()))
    cur.close()
    con.commit()


def insert_triplets(con: sqlite3.Connection, trips: Set):
    cur = con.cursor()
    cur.executemany('INSERT INTO triplets (hop1, hop2, hop3) VALUES (?, ?, ?)', trips)
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
