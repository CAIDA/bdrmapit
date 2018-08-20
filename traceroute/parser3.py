import json
import os
import sqlite3
from collections import Counter
from typing import Set

from traceroute.atlas_trace import AtlasTrace
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


def opendb(filename, remove=False):
    if remove:
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
    con = sqlite3.connect(filename)
    cur = con.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS address (addr TEXT, num INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS adjacency (hop1 TEXT, hop2 TEXT, distance INT, type INT, direction INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS destpair (addr TEXT, asn INT, echo BOOLEAN, exclude INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS distance (hop1 TEXT, hop2 TEXT, distance INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS seen (hop1 TEXT, hop2 TEXT, num INT)')
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
        if self.output_type == OutputType.warts:
            with Warts(self.filename, json=True) as f:
                for j in f:
                    if j['type'] == 'trace':
                        yield WartsTrace(j, ip2as=self.ip2as)
        elif self.output_type == OutputType.atlas:
            with File2(self.filename) as f:
                for j in map(json.loads, f):
                    yield AtlasTrace(j, ip2as=self.ip2as)

    def parseone(self, trace):
        self.addrs.update((h.addr, h.num) for h in trace.allhops if not h.private)
        dest_asn = trace.dst_asn
        numhops = len(trace.hops)
        if numhops == 0:
            return
        i = 0
        y = trace.hops[0]
        if not y.private:
            self.dps.add((y.addr, dest_asn, y.icmp_type == 0, NONE))
        while i < numhops - 1:
            i += 1
            x, y, z = y, trace.hops[i], (trace.hops[i + 1] if i < numhops - 1 else None)
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
            # if y.ttl == z.ttl - 1:
            #     yzdiff = y.num - z.num
            #     if yzdiff == 1 or yzdiff == -1:
            #         self.adjs.add((x.addr, z.addr, distance, z.icmp_type, BOTH))
            #         self.adjs.add((y.addr, x.addr, distance, x.icmp_type, FORWARD))
            #         self.adjs.add((y.addr, z.addr, distance, z.icmp_type, BACKWARD))
            #         self.dps.add((y.addr, dest_asn, y.icmp_type == 0, ADJEXCLUDE))
            #         self.dps.add((z.addr, dest_asn, z.icmp_type == 0, NONE))
            #         self.dists[(x.addr, z.addr)] += 1 if distance == 1 else -1
            #         self.dists[(y.addr, x.addr)] += 1 if distance == 1 else -1
            #         self.dists[(y.addr, z.addr)] += 1 if distance == 1 else -1
            #         y = z
            #         i += 1
            #         continue
            self.adjs.add((x.addr, y.addr, distance, y.icmp_type, BOTH))
            self.dps.add((y.addr, dest_asn, y.icmp_type == 0, NONE))
            self.dists[(x.addr, y.addr)] += 1 if distance == 1 else -1

    def parse(self):
        for trace in self:
            self.addrs.update((h.addr, h.num) for h in trace.allhops if not h.private)
            dest_asn = trace.dst_asn
            numhops = len(trace.hops)
            if numhops == 0:
                continue
            i = 0
            y = trace.hops[0]
            if not y.private:
                self.dps.add((y.addr, dest_asn, y.icmp_type == 0, NONE))
            while i < numhops - 1:
                i += 1
                x, y, z = y, trace.hops[i], (trace.hops[i + 1] if i < numhops - 1 else None)
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
                # if y.ttl == z.ttl - 1:
                #     yzdiff = y.num - z.num
                #     if yzdiff == 1 or yzdiff == -1:
                #         self.adjs.add((x.addr, z.addr, distance, z.icmp_type, BOTH))
                #         self.adjs.add((y.addr, x.addr, distance, x.icmp_type, FORWARD))
                #         self.adjs.add((y.addr, z.addr, distance, z.icmp_type, BACKWARD))
                #         self.dps.add((y.addr, dest_asn, y.icmp_type == 0, ADJEXCLUDE))
                #         self.dps.add((z.addr, dest_asn, z.icmp_type == 0, NONE))
                #         self.dists[(x.addr, z.addr)] += 1 if distance == 1 else -1
                #         self.dists[(y.addr, x.addr)] += 1 if distance == 1 else -1
                #         self.dists[(y.addr, z.addr)] += 1 if distance == 1 else -1
                #         y = z
                #         i += 1
                #         continue
                self.adjs.add((x.addr, y.addr, distance, y.icmp_type, BOTH))
                self.dps.add((y.addr, dest_asn, y.icmp_type == 0, NONE))
                self.dists[(x.addr, y.addr)] += 1 if distance == 1 else -1

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
    cur.executemany('INSERT INTO address (addr, num) VALUES (?, ?)', addrs)
    cur.close()
    con.commit()


def insert_adjacency(con: sqlite3.Connection, adjs: Set):
    cur = con.cursor()
    cur.executemany('INSERT INTO adjacency (hop1, hop2, distance, type, direction) VALUES (?, ?, ?, ?, ?)', adjs)
    cur.close()
    con.commit()


def insert_destpair(con: sqlite3.Connection, dps: Set):
    cur = con.cursor()
    cur.executemany('INSERT INTO destpair (addr, asn, echo, exclude) VALUES (?, ?, ?, ?)', dps)
    cur.close()
    con.commit()


def insert_distance(con: sqlite3.Connection, dists: Counter):
    cur = con.cursor()
    cur.executemany('INSERT INTO distance (hop1, hop2, distance) VALUES (?, ?, ?)', ((x, y, n) for (x, y), n in dists.items()))
    cur.close()
    con.commit()
