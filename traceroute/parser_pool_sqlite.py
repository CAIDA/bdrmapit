import os
import select
import sqlite3
import sys
from argparse import ArgumentParser, FileType, Namespace
from collections import Counter
from multiprocessing import Queue, Process, Lock
from typing import List

import traceroute.parser as tp
from bgp.routing_table import RoutingTable
from traceroute.output_type import OutputType
from traceroute.parser import Parser, insert_address, insert_adjacency, insert_destpair, insert_distance

filesq = Queue()
addrq = Queue()
adjq = Queue()
dpq = Queue()
distq = Queue()
progressq = Queue()
ip2as: RoutingTable = None
combine_lock = Lock()
args: Namespace = None


def worker():
    while True:
        args = filesq.get()
        if not args:
            break
        filename, output_type = args
        p = Parser(filename, output_type, ip2as)
        ofilename = os.path.splitext(os.path.basename(filename))[0]
        ofile = os.path.join(args.output_dir, 'tmp', ofilename + '.db')
        p.to_sql(ofile)
        progressq.put(('worker', ofile))


def _combine(queue, table):
    while True:
        filename = queue.get()
        if not filename:
            break
        con = sqlite3.connect(filename)
        cur = con.cursor()
        yield cur.execute('SELECT * FROM {}'.format(table))
        cur.close()
        con.close()


def combine_addrs(output):
    addrs = set()
    for results in _combine(addrq, tp.ADDR):
        addrs.update(results)
    with combine_lock:
        con = sqlite3.connect(output)
        insert_address(con, addrs)
        con.close()


def combine_adjs(output):
    adjs = set()
    for results in _combine(adjq, tp.ADJ):
        adjs.update(results)
    with combine_lock:
        con = sqlite3.connect(output)
        insert_adjacency(con, adjs)
        con.close()


def combine_dps(output):
    dps = set()
    for results in _combine(dpq, tp.DP):
        dps.update(results)
    with combine_lock:
        con = sqlite3.connect(output)
        insert_destpair(con, dps)
        con.close()


def combine_dists(output):
    dists = Counter()
    for results in _combine(distq, tp.DIST):
        for x, y, n in results:
            dists[x, y] += n
    with combine_lock:
        con = sqlite3.connect(output)
        insert_distance(con, dists)
        con.close()


def mkdirs():
    os.makedirs(os.path.join(args.output_dir, 'tmp'), exist_ok=True)


def run(files, poolsize, no_combine):
    completed = 0
    addrs = 0
    adjs = 0
    dps = 0
    dists = 0
    mkdirs()
    combined = os.path.join(args.output_dir, 'combined.db')
    for filename in files:
        filesq.put(filename)
    rlist = [progressq]
    workers: List[Process] = []
    for _ in range(poolsize):
        p = Process(target=worker)
        p.start()
        rlist.append(p.sentinel)
        workers.append(p)
        filesq.put(None)
    if not no_combine:
        tp.opendb(combined, remove=True)
        p = Process(target=combine_addrs, args=(combined,))
        rlist.append(p.sentinel)
        p.start()
        p = Process(target=combine_adjs, args=(combined,))
        rlist.append(p.sentinel)
        p.start()
        p = Process(target=combine_dps, args=(combined,))
        rlist.append(p.sentinel)
        p.start()
        p = Process(target=combine_dists, args=(combined,))
        rlist.append(p.sentinel)
        p.start()
    while True:
        ready, _, _ = select.select(rlist, [], [])
        for reader in ready:
            if ready == progressq:
                wtype, value = progressq.get()
                if wtype == 'worker':
                    completed += 1
                    if not no_combine:
                        addrq.put(value)
                        adjq.put(value)
                        dpq.put(value)
                        distq.put(value)
                elif wtype == tp.ADDR:
                    addrs = value
                elif wtype == tp.ADJ:
                    adjs = value
                elif wtype == tp.DP:
                    dps = value
                else:
                    dists = value
                sys.stderr.write(
                    '\r\033[KWorkers {:,d} Addrs {:,d} Adjs {:,d} DPs {:,d} Dists {:,d}'.format(completed, addrs, adjs,
                                                                                                dps, dists))
            else:
                rlist.remove(reader)
        if len(rlist) == 1:
            sys.stderr.write('\n')
            break


def read_filenames(f, output_type):
    for filename in map(str.strip, f):
        if filename:
            yield (filename, output_type)


def main(vargs=None):
    global ip2as, args
    parser = ArgumentParser()
    parser.add_argument('-W', '--warts', type=FileType('r'), help='File containing warts filenames.')
    parser.add_argument('-w', '--single-warts', help='Single warts file.')
    parser.add_argument('-A', '--atlas', type=FileType('r'), help='File containing atlas filenames.')
    parser.add_argument('-k', '--keep', action='store_true', help='Keep the intermediate files.')
    parser.add_argument('-o', '--output-dir', default='.', help='Directory where the output files will be written.')
    parser.add_argument('-p', '--poolsize', default=1, type=int, help='Number of parallel processes.')
    parser.add_argument('-i', '--ip2as', required=True, help='BGP prefix file regex to use.')
    parser.add_argument('--no-combine', action='store_true', help='Prevent combining outputs.')
    if vargs:
        args = parser.parse_args(vargs)
    else:
        args = parser.parse_args()
    files = []
    if args.single_warts:
        files.append((args.single_warts, OutputType.warts))
    if args.warts:
        files.extend(read_filenames(args.warts, OutputType.warts))
        print('Number of warts files {:,d}'.format(len(files)))
    if args.atlas:
        atlas_files = list(read_filenames(args.atlas, OutputType.atlas))
        print('Number of atlas files {:,d}'.format(len(atlas_files)))
        files.extend(atlas_files)
    print('Number of files {:,d}'.format(len(files)))
    ip2as = RoutingTable.ip2as(args.ip2as) if ip2as is None else ip2as
    run(files, args.poolsize, args.no_combine)
    print('Cleaning up...')
