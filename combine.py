#!/usr/bin/env python
import sqlite3
from argparse import ArgumentParser, FileType
from collections import Counter

from traceroute.parser import opendb
from utils.progress import Progress


def combinedbs(files, output):
    addrs = set()
    adjs = set()
    dps = set()
    dists = Counter()
    pb = Progress(len(files), 'Combining databases', callback=lambda: 'Addrs {:,d} Adjs {:,d} DPs {:,d} Dists {:,d}'.format(len(addrs), len(adjs), len(dps), len(dists)))
    for filename in pb.iterator(files):
        try:
            con = sqlite3.connect(filename)
        except sqlite3.OperationalError:
            print(filename)
            raise
        cur = con.cursor()
        addrs.update(cur.execute('SELECT addr FROM address'))
        adjs.update(cur.execute('SELECT hop1, hop2, distance, type, special FROM adjacency'))
        dps.update(cur.execute('SELECT addr, asn FROM destpair'))
        dists.update({(h1, h2): num for h1, h2, num in cur.execute('SELECT hop1, hop2, distance FROM distance')})
        cur.close()
        con.close()
    con = opendb(output, remove=True)
    cur = con.cursor()
    cur.executemany('INSERT INTO address (addr) VALUES (?)', addrs)
    cur.executemany('INSERT INTO adjacency (hop1, hop2, distance, type, special) VALUES (?, ?, ?, ?, ?)', adjs)
    cur.executemany('INSERT INTO destpair (addr, asn) VALUES (?, ?)', dps)
    cur.executemany('INSERT INTO distance (hop1, hop2, distance) VALUES (?, ?, ?)', ((h1, h2, n) for (h1, h2), n in dists.items()))
    cur.close()
    con.commit()
    con.close()


def main():
    parser = ArgumentParser()
    parser.add_argument('-o', '--output', required=True)
    parser.add_argument('-f', '--files', required=True, type=FileType('r'))
    args = parser.parse_args()
    files = [line.strip() for line in args.files]
    combinedbs(files, args.output)


if __name__ == '__main__':
    main()