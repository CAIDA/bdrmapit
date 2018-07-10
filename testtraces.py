#!/usr/bin/env python
import json
import sqlite3
from argparse import ArgumentParser
from multiprocessing import Lock
from multiprocessing.pool import Pool
from typing import Set

from traceroute.warts import Warts
from utils.progress import Progress
from utils.utils import File2

addrs: Set[str] = None
output: str = None
dblock = Lock()


def create():
    con = sqlite3.connect(output)
    cur = con.cursor()
    cur.execute('''CREATE TABLE found (
  addr TEXT,
  trace TEXT,
  filename TEXT
)''')
    cur.close()
    con.commit()


def find(filename):
    found = []
    with Warts(filename, json=False) as f:
        for line in f:
            line = line.rstrip()
            try:
                j = json.loads(line)
                hops = j.get('hops')
                if hops:
                    for hop in hops:
                        addr = hop['addr']
                        if addr in addrs:
                            found.append((addr, line, filename))
            except json.JSONDecodeError:
                pass
    with dblock:
        con = sqlite3.connect(output)
        cur = con.cursor()
        cur.executemany('INSERT INTO found VALUES (?, ?, ?)', found)
        cur.close()
        con.commit()
    return len(found)


def main():
    global addrs, output
    parser = ArgumentParser()
    parser.add_argument('-W', '--warts')
    parser.add_argument('-a', '--addrs')
    parser.add_argument('-o', '--output')
    parser.add_argument('-p', '--poolsize', type=int)
    args = parser.parse_args()
    output = args.output
    create()
    with File2(args.addrs) as f:
        addrs = {line.strip() for line in f}
    print('Addrs: {:,d}'.format(len(addrs)))
    output = args.output
    with File2(args.warts) as f:
        files = [line.strip() for line in f]
    print('Files: {:,d}'.format(len(files)))
    with Pool(args.poolsize) as pool:
        total = 0
        pb = Progress(len(files), 'Finding test addrs', callback=lambda: 'Found {:,d}'.format(total))
        for added in pb.iterator(pool.imap_unordered(find, files)):
            total += added


if __name__ == '__main__':
    main()
