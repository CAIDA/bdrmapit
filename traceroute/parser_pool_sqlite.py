import os
import sqlite3
import sys
from argparse import ArgumentParser, FileType
from collections import Counter, namedtuple
from logging import getLogger, INFO
from multiprocessing import Queue, Process
from traceback import TracebackException
from typing import List, Set

import bgp.routing_table as rt
import traceroute.abstract_parser as ap
from utils.progress import Progress

log = getLogger()
log.setLevel(INFO)

TB = namedtuple('TB', ['value'])
args = None


class CustomException(Exception):
    pass


def opendb(filename, remove=False):
    if remove:
        try:
            os.remove(filename)
        except FileNotFoundError:
            pass
    con = sqlite3.connect(filename)
    cur = con.cursor()
    cur.execute('CREATE TABLE IF NOT EXISTS address (addr TEXT)')
    cur.execute('CREATE TABLE IF NOT EXISTS adjacency (hop1 TEXT, hop2 TEXT, distance INT, type INT)')
    cur.execute('CREATE TABLE IF NOT EXISTS destpair (addr TEXT, asn INT, echo BOOLEAN, loop BOOLEAN)')
    cur.execute('CREATE TABLE IF NOT EXISTS distance (hop1 TEXT, hop2 TEXT, distance INT)')
    cur.close()
    con.commit()
    return con


def worker_func(jobs: Queue, outputs: Queue):
    try:
        while True:
            filename = jobs.get()
            if filename is None:
                break
            filename, output_type = filename
            process_trace_file(filename, output_type, outputs)
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = TracebackException(exc_type, exc_value, exc_traceback)
        t = TB(''.join(tb.format()))
        outputs.put(t)


def process_trace_file(filename: str, output_type: ap.OutputType, outputs=None):
    addresses = set()
    adjacencies = set()
    destpairs = set()
    distances = Counter()
    for trace in ap.Parser(filename, output_type):
        if trace:
            addresses.update((addr,) for addr in trace.addresses)
            hopslist = trace.hopslist
            if hopslist:
                numhops = len(hopslist)
                dest_asn = trace.dst_asn
                loop = trace.stop_reason == 'LOOP'
                i = 0
                hop2 = hopslist[0]
                qttl2 = hop2.qttl
                addr2 = hop2.addr
                if hop2.asn > -2:
                    destpairs.add((addr2, dest_asn, hop2.icmp_type == 0, loop))
                while i < numhops - 1:
                    i += 1
                    hop1, hop2 = hop2, hopslist[i]
                    hop3 = hopslist[i + 1] if i <= numhops - 2 else None
                    qttl1, qttl2 = qttl2, hop2.qttl
                    addr1, addr2 = addr2, hop2.addr
                    distance = hop2.ttl - hop1.ttl
                    distances[(addr1, addr2)] += 1 if distance == 1 else -1
                    if hop3:
                        distances[(addr1, hop3.addr)] -= 1
                    if hop2.asn > -2:
                        destpairs.add((addr2, dest_asn, hop2.icmp_type == 0, loop))
                    if addr1 != addr2:  # Skip links with the same address
                        if qttl2 == 0:
                            if not (hop3 and (addr2 == hop3.addr or hop2.reply_ttl - hop3.reply_ttl == (hop3.ttl - hop2.ttl) - 1)):
                                qttl2 = 1
                        elif qttl2 > 1:
                            if not (hop2.icmp_type == 3 and hop2.qttl - hop1.qttl >= hop2.ttl - hop1.ttl):
                                qttl1 = 1
                                qttl2 = 1
                        distance -= qttl2 - qttl1
                        if distance == 1:
                            link_type = 1
                        elif distance <= 0:
                            link_type = -1
                        else:
                            link_type = 2
                        adjacencies.add((addr1, addr2, link_type, hop2.icmp_type))
    write_tmps(filename, destpairs, distances, adjacencies, addresses, outputs=outputs)


def mkdirs():
    os.makedirs(os.path.join(args.output_dir, 'tmp'), exist_ok=True)


def run(files: List[str], poolsize: int):
    addrs = set()
    adjs = set()
    dps = set()
    dists = Counter()
    mkdirs()
    if len(files) > 1:
        jobs = Queue()  # Traceroute filenames will be put here for the works
        outputs = Queue()  # Workers put completed filenames here for aggs to read
        workers = {}
        for i in range(poolsize):
            p = Process(target=worker_func, args=(jobs, outputs), name='Jobs')
            p.start()
            workers[p.pid] = p
        try:
            # Put filenames into the jobs queue
            for filename in files:
                jobs.put(filename)
            # Put the None markers into the jobs queue, one for each worker
            for _ in range(poolsize):
                jobs.put(None)
            pb = Progress(len(files), 'Parsing traceroutes', increment=1, callback=lambda: 'Addrs {:,d} Adjs {:,d} DPs {:,d} Dists {:,d}'.format(len(addrs), len(adjs), len(dps), len(dists)))
            for _ in files:
                filename = outputs.get()
                if isinstance(filename, TB):
                    print(filename.value)
                    raise CustomException('Custom')
                if not args.no_combine:
                    con = opendb(filename, False)
                    cur = con.cursor()
                    addrs.update(row for row in cur.execute('SELECT * FROM address'))
                    adjs.update(row for row in cur.execute('SELECT * FROM adjacency'))
                    dps.update(row for row in cur.execute('SELECT * FROM destpair'))
                    dists.update({(hop1, hop2): n for hop1, hop2, n in cur.execute('SELECT hop1, hop2, distance FROM distance')})
                    cur.close()
                    con.close()
                pb.inc()
            pb.finish()
        except CustomException:
            for k, p in workers.items():
                p.terminate()
                p.join()
        finally:
            jobs.close()
            outputs.close()
            write_tmps('combined.db', dps, dists, adjs, addrs)
    else:
        pb = Progress(len(files))
        for filename, output_type in pb.iterator(files):
            process_trace_file(filename, output_type)


def todb(engine, query, values, chunksize=100000):
    tmp = []
    i = 0
    for v in values:
        tmp.append(dict(v))
        i += 1
        if i == chunksize:
            engine.execute(query, tmp)
            tmp = []
            i = 0
    if tmp:
        engine.execute(query, tmp)


def write_tmps(filename, dps: Set, dists: Counter, adjs: Set, addrs: Set, outputs=None):
    ofilename = os.path.splitext(os.path.basename(filename))[0]
    ofile = os.path.join(args.output_dir, ofilename + '.db')
    con = opendb(ofile, remove=True)
    cur = con.cursor()
    cur.executemany('INSERT INTO address (addr) VALUES (?)', addrs)
    cur.executemany('INSERT INTO adjacency (hop1, hop2, distance, type) VALUES (?, ?, ?, ?)', adjs)
    cur.executemany('INSERT INTO destpair (addr, asn, echo, loop) VALUES (?, ?, ?, ?)', dps)
    cur.executemany('INSERT INTO distance (hop1, hop2, distance) VALUES (?, ?, ?)', ((h1, h2, n) for (h1, h2), n in dists.items()))
    cur.close()
    con.commit()
    con.close()
    if outputs is not None:
        outputs.put(ofile)


def read_filenames(f, output_type):
    for filename in map(str.strip, f):
        if filename:
            yield (filename, output_type)


def main(vargs=None, ip2as=None):
    global args
    parser = ArgumentParser()
    parser.add_argument('-a', '--adj', action='store_false', help='Adjacency output file.')
    parser.add_argument('-b', '--addr', action='store_false', help='Addresses output file.')
    parser.add_argument('-d', '--dp', action='store_false', help='Dest pairs output file.')
    parser.add_argument('-e', '--dist', action='store_false', help='Distances between interfaces.')
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
        files.append((args.single_warts, ap.OutputType.warts))
    if args.warts:
        files.extend(read_filenames(args.warts, ap.OutputType.warts))
        log.info('Number of warts files {:,d}'.format(len(files)))
    if args.atlas:
        atlas_files = list(read_filenames(args.atlas, ap.OutputType.atlas))
        log.info('Number of atlas files {:,d}'.format(len(atlas_files)))
        files.extend(atlas_files)
    log.info('Number of files {:,d}'.format(len(files)))
    ap.ip2as = rt.RoutingTable.ip2as(args.ip2as) if ip2as is None else ip2as
    run(files, args.poolsize)
    print('Cleaning up...')
