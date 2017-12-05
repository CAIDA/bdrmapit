import csv
import os
import sys
from argparse import ArgumentParser, FileType
from collections import Counter, namedtuple
from logging import getLogger, INFO
from multiprocessing import Queue, Process, current_process
from sys import stderr
from traceback import TracebackException

import pandas as pd

import bgp.routing_table as rt
import traceroute.abstract_parser as ap
from utils.progress import Progress

log = getLogger()
log.setLevel(INFO)

Adjacency = namedtuple('Adjacency', ['Hop1', 'Hop2', 'Distance', 'Private', 'Suspicious', 'Type'])
DestPair = namedtuple('DestPair', ['Interface', 'DestASN', 'Echo', 'Suspicious', 'Loop'])
Distance = namedtuple('Distance', ['Hop1', 'Hop2', 'Distance'])
Outputs = namedtuple('Outputs', ['Addrs', 'Adjs', 'DPs', 'Dists'])
TB = namedtuple('TB', ['value'])
args = None


def worker_func(jobs, outputs, status):
    try:
        p = current_process()
        pid = p.pid
        name = p.name
        while True:
            filename = jobs.get()
            if filename is None:
                break
            filename, output_type = filename
            process_trace_file(filename, output_type, outputs)
            status.put((pid, name, 1))
        for q in outputs:
            q.put(None)
        status.put((pid, None, 1))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = TracebackException(exc_type, exc_value, exc_traceback)
        t = TB(''.join(tb.format()))
        status.put(t)


def combine(fqueue, ofile, status, poolsize):
    try:
        header = ''
        p = current_process()
        pid = p.pid
        name = p.name
        s = set()
        while True:
            filename = fqueue.get()
            if filename is None:
                poolsize -= 1
                if poolsize == 0:
                    break
                continue
            with open(filename) as f:
                header = f.readline()
                s.update(f)
            if not args.keep:
                os.remove(filename)
            status.put((pid, name, len(s)))
        with open(ofile, 'w') as f:
            f.write(header)
            f.writelines(s)
        status.put((pid, None, len(s)))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = TracebackException(exc_type, exc_value, exc_traceback)
        t = TB(''.join(tb.format()))
        status.put(t)


def combine_dists(fqueue, ofile, status, poolsize):
    try:
        p = current_process()
        pid = p.pid
        name = p.name
        s = Counter()
        while True:
            filename = fqueue.get()
            if filename is None:
                poolsize -= 1
                if poolsize == 0:
                    break
                continue
            for hop1, hop2, n in pd.read_csv(filename).itertuples(index=False, name=None):
                s[(hop1, hop2)] += n
            if not args.keep:
                os.remove(filename)
            status.put((pid, name, len(s)))
        write_distances(ofile, s, nonegative=True)
        status.put((pid, None, len(s)))
    except:
        exc_type, exc_value, exc_traceback = sys.exc_info()
        tb = TracebackException(exc_type, exc_value, exc_traceback)
        t = TB(''.join(tb.format()))
        status.put(t)


def create_combiners(func, queue, filename, status, poolsize, name, workers, cj, sizes):
    p = Process(target=func, args=(queue, filename, status, poolsize), name=name)
    p.start()
    workers[p.pid] = p
    cj[name] = 0
    sizes[name] = 0


def mkdirs():
    os.makedirs(os.path.join(args.output_dir, 'addrs'), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'adjs'), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'dps'), exist_ok=True)
    os.makedirs(os.path.join(args.output_dir, 'dists'), exist_ok=True)


def process_trace_file(filename, output_type, outputs=None):
    addresses = ap.Set2(args.addr)
    adjacencies = ap.Set2(args.adj)
    destpairs = ap.Set2(args.dp)
    distances = ap.Counter2(args.dist)
    for trace in ap.Parser(filename, output_type):
        if trace:
            addresses.update(trace.addresses)
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
                    destpairs.add((addr2, dest_asn, hop2.icmp_type == 0, False, loop))
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
                    issuspicious = hop2.ttl > trace.suspicious_hop(dest_asn)
                    if hop2.asn > -2:
                        destpairs.add((addr2, dest_asn, hop2.icmp_type == 0, issuspicious, loop))
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
                        adjacencies.add((addr1, addr2, link_type, hop1.private, issuspicious, hop2.icmp_type))
    write_tmps(filename, destpairs, distances, adjacencies, addresses, outputs=outputs)


def run(files, poolsize):
    if len(files) > 1:
        mkdirs()
        jobs = Queue()  # Traceroute filenames will be put here for the works
        outputs = Outputs(*[Queue() for _ in Outputs._fields])  # Workers put completed filenames here for aggs to read
        status = Queue()
        workers = {}
        cj = {'Jobs': 0}
        sizes = {}
        for i in range(poolsize):
            p = Process(target=worker_func, args=(jobs, outputs, status), name='Jobs')
            p.start()
            workers[p.pid] = p
        if args.addr:
            create_combiners(combine, outputs.Addrs, os.path.join(args.output_dir, 'addrs.txt'), status, poolsize,
                             'Addrs', workers, cj, sizes)
        if args.adj:
            create_combiners(combine, outputs.Adjs, os.path.join(args.output_dir, 'adjs.csv'), status, poolsize, 'Adjs',
                             workers, cj, sizes)
        if args.dp:
            create_combiners(combine, outputs.DPs, os.path.join(args.output_dir, 'dps.csv'), status, poolsize, 'DPs',
                             workers, cj, sizes)
        if args.dist:
            create_combiners(combine_dists, outputs.Dists, os.path.join(args.output_dir, 'dists.csv'), status, poolsize,
                             'Dists', workers, cj, sizes)
        try:
            # Put filenames into the jobs queue
            for filename in files:
                jobs.put(filename)
            # Put the None markers into the jobs queue, one for each worker
            for _ in range(poolsize):
                jobs.put(None)
            # Track the status
            while workers:
                a = status.get()
                if isinstance(a, Exception):
                    raise a
                if isinstance(a, TB):
                    print(a.value)
                    raise Exception('Custom')
                pid, job, num = a
                if job is None:
                    p = workers.pop(pid)
                    p.join()
                else:
                    cj[job] += 1
                    if job != 'Jobs':
                        sizes[job] = num
                s = []
                for i, n in cj.items():
                    s.append('{} {:,d} ({:.2%}) {:,d}'.format(i, n, n / len(files), sizes.get(i, 0)))
                sys.stderr.write('\r\033[K{}'.format(' '.join(s)))
            sys.stderr.write('\n')
        finally:
            for k, p in workers.items():
                print(k)
                p.terminate()
                p.join()
    else:
        mkdirs()
        pb = Progress(len(files))
        for filename in pb.iterator(files):
            process_trace_file(*filename)


def write_tmps(filename, destpairs, distances, adjacencies, addresses, outputs=None):
    ofilename = os.path.splitext(os.path.basename(filename))[0]
    if destpairs:
        ofile = os.path.join(args.output_dir, 'dps', ofilename + '.csv')
        write_destpairs(ofile, destpairs)
        if outputs is not None:
            outputs.DPs.put(ofile)
    if distances:
        ofile = os.path.join(args.output_dir, 'dists', ofilename + '.csv')
        write_distances(ofile, distances)
        if outputs is not None:
            outputs.Dists.put(ofile)
    if adjacencies:
        ofile = os.path.join(args.output_dir, 'adjs', ofilename + '.csv')
        write_adjacencies(ofile, adjacencies)
        if outputs is not None:
            outputs.Adjs.put(ofile)
    if addresses:
        ofile = os.path.join(args.output_dir, 'addrs', ofilename + '.txt')
        write_addresses(ofile, addresses)
        if outputs is not None:
            outputs.Addrs.put(ofile)


def write_addresses(filename, addresses):
    with open(filename, 'w') as f:
        f.write('Addresses\n')
        f.writelines('{}\n'.format(address) for address in addresses)


def write_adjacencies(filename, adjacencies):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(Adjacency._fields)
        writer.writerows(adjacencies)


def write_destpairs(filename, destpairs):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(DestPair._fields)
        writer.writerows(destpairs)


def write_distances(filename, distances, nonegative=False):
    with open(filename, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(Distance._fields)
        if nonegative:
            writer.writerows([[hop1, hop2, num] for (hop1, hop2), num in distances.items() if num > 0])
        else:
            writer.writerows([[hop1, hop2, num] for (hop1, hop2), num in distances.items()])


def read_filenames(f, output_type):
    for filename in map(str.strip, f):
        if filename:
            yield (filename, output_type)


def main(vargs=None, ip2as=None):
    global args
    parser = ArgumentParser()
    parser.add_argument('-a', '--adj', action='store_true', help='Adjacency output file.')
    parser.add_argument('-b', '--addr', action='store_true', help='Addresses output file.')
    parser.add_argument('-d', '--dp', action='store_true', help='Dest pairs output file.')
    parser.add_argument('-e', '--dist', action='store_true', help='Distances between interfaces.')
    parser.add_argument('-W', '--warts', type=FileType('r'), help='File containing warts filenames.')
    parser.add_argument('-A', '--atlas', type=FileType('r'), help='File containing atlas filenames.')
    parser.add_argument('-k', '--keep', action='store_true', help='Keep the intermediate files.')
    parser.add_argument('-o', '--output-dir', default='.', help='Directory where the output files will be written.')
    parser.add_argument('-p', '--poolsize', default=1, type=int, help='Number of parallel processes.')
    parser.add_argument('-i', '--ip2as', required=True, help='BGP prefix file regex to use.')
    if vargs:
        args = parser.parse_args(vargs)
    else:
        args = parser.parse_args()
    files = []
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
    print('Cleaning up...', file=stderr)
