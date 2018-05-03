from argparse import ArgumentParser, FileType
from collections import Counter, defaultdict
from multiprocessing.pool import Pool
from subprocess import Popen, PIPE

import sys

from utils.progress import Progress


def origins(origin):
    try:
        origin = int(origin)
        return [origin]
    except ValueError:
        if origin[0] == '{':
            return [asn for o in origin[1:-1].split(',') for asn in origins(o)]


def parserib(filename: str):
    counter = {}
    cmd = 'bgpreader -d singlefile -o rib-file,{} -w 0,2147483648'.format(filename)
    p = Popen(cmd, shell=True, stdout=PIPE, universal_newlines=True)
    for line in p.stdout:
        fields = line.split('|')
        if len(fields) >= 10:
            prefix, _, prefixlen = fields[7].partition('/')
            prefixlen = int(prefixlen)
            if 8 <= prefixlen <= 25:
                origin_as = fields[10]
                t = (prefix, prefixlen, origin_as)
                counter[t] = counter.get(t, 0) + 1
    return counter


def parallel_parsing(files, poolsize):
    prefixes = Counter()
    pb = Progress(len(files), 'Parsing RIBs', callback=lambda: 'Prefixes {:,d}'.format(len(prefixes)))
    with Pool(poolsize) as pool:
        for counter in pb.iterator(pool.imap_unordered(parserib, files)):
            prefixes.update(counter)
    return prefixes


def organize_prefixes(prefixes_counter):
    prefixes = defaultdict(dict)
    pb = Progress(len(prefixes), 'Organizing prefixes', increment=100000, callback=lambda: 'Used {:,d}'.format(len(prefixes)))
    for (address, prefixlen, asn), n in pb.iterator(prefixes_counter.items()):
        prefixes[(address, prefixlen)][asn] = n
    return prefixes


def write_prefixes(f, prefixes):
    for (address, prefixlen), origins in sorted(prefixes.items(), key=lambda x: x[1][1]):
        neworigins = []
        for origin, _ in origins.most_common():
            if '{' in origin:
                origin = origin[1:-1]
            neworigins.append(origin)
        f.write('{}\t{}\t{}\n'.format(address, prefixlen, '_'.join(neworigins)))


def main():
    parser = ArgumentParser()
    parser.add_argument('-p', '--poolsize', type=int, default=30)
    parser.add_argument('-o', '--output', type=FileType('w'), default=sys.stdout)
    parser.add_argument('files', type=FileType('r'))
    args = parser.parse_args()
    files = [line.strip() for line in args.files]
    prefixes = parallel_parsing(files, args.poolsize)
    prefixes = organize_prefixes(prefixes)
    write_prefixes(args.output, prefixes)


if __name__ == '__main__':
    main()
