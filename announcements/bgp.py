import re
from collections import Counter, defaultdict

import multiprocessing

from announcements.ribs import RIB
import pandas as pd

from utils.progress import Progress


# def extract_origin(filename, increment=1000000, chunksize=10000):
#     c = Counter()
#     r = re.compile(r'(\d+\.\d+\.\d+\.\d+)/(\d+)')
#     with RIB(filename) as f:
#     # with open(filename) as f:
#         for line in f:
#             fields = line.split('|', 7)
#             prefix, path = fields[5:7]
#             if prefix and path:
#                 m = r.match(prefix)
#                 if m:
#                     origin = path.rpartition(' ')[-1]
#                     if origin[0] == '{':
#                         if ',' not in origin:
#                             origin = origin[1:-1]
#                         else:
#                             continue
#                     origin = int(origin)
#                     if 0 < origin < 64496 or 131071 < origin < 4200000000:
#                         address, prefixlen = m.groups()
#                         prefixlen = int(prefixlen)
#                         if 0 < prefixlen <= 24:
#                             c[(address, prefixlen, origin)] += 1
#     return c


def extract_origin(filename, increment=1000000, chunksize=10000):
    c = Counter()
    r = re.compile(r'(\d+\.\d+\.\d+\.\d+)/(\d+)')
    with RIB(filename) as f:
        # print(filename)
        for line in f:
            # print(line)
            fields = line.split('|', 7)
            prefix, path = fields[5:7]
            # print(prefix, path)
            if prefix and path:
                m = r.match(prefix)
                if m:
                    origin = path.rpartition(' ')[-1]
                    # if origin[0] == '{':
                    #     if ',' not in origin:
                    #         origin = origin[1:-1]
                    #     else:
                    #         continue
                    # origin = int(origin)
                    # if 0 < origin < 64496 or 131071 < origin < 4200000000:
                    address, prefixlen = m.groups()
                    # print(address, prefixlen, origin)
                    prefixlen = int(prefixlen)
                    if 0 < prefixlen <= 24:
                        c[(address, prefixlen, origin)] += 1
    return c


def parse_files(files, poolsize=-1):
    c = Counter()
    if poolsize >= 0:
        pool = multiprocessing.Pool(poolsize)
        results = pool.imap_unordered(extract_origin, files)
    else:
        results = map(extract_origin, files)
    pb = Progress(len(files), 'Extracting prefixes', callback=lambda: 'Total {:,d}'.format(len(c)))
    for newc in pb.iterator(results):
        c.update(newc)
    if poolsize >= 0:
        pool.close()
    return c


def by_prefix(prefixes):
    pdict = defaultdict(Counter)
    for (address, prefixlen, asn), n in prefixes.items():
        pdict[(address, prefixlen)][asn] += n
    return pdict


def count_orgs(counter, as2org):
    orgs = defaultdict(list)
    for asn in counter:
        orgs[as2org[asn]].append(asn)
    return orgs


def organize_prefixes(prefixes_counter):
    prefixes = defaultdict(Counter)
    pb = Progress(len(prefixes), 'Organizing prefixes', increment=100000, callback=lambda: 'Used {:,d}'.format(len(prefixes)))
    for (address, prefixlen, asn), n in pb.iterator(prefixes_counter.items()):
            prefixes[(address, prefixlen)][asn] = n
    return prefixes


def write_prefixes(filename, prefixes):
    with open(filename, 'w') as f:
        for (address, prefixlen), origins in sorted(prefixes.items(), key=lambda x: x[1][1]):
            neworigins = []
            for origin, _ in origins.most_common():
                if '{' in origin:
                    origin = origin[1:-1]
                neworigins.append(origin)
            f.write('{}\t{}\t{}\n'.format(address, prefixlen, '_'.join(neworigins)))
