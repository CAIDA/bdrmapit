import multiprocessing
import re
import sys
from argparse import ArgumentParser, FileType
from collections import Counter, defaultdict
from subprocess import Popen, PIPE

from utils.progress import Progress
from utils.utils import read_filenames


def extract_origin(filename):
    c = Counter()
    r = re.compile(r'(\d+\.\d+\.\d+\.\d+)/(\d+)')
    p = Popen('bgpdump -m {}'.format(filename), shell=True, stdout=PIPE, stderr=PIPE, universal_newlines=True)
    for line in p.stdout:
        fields = line.split('|', 7)
        prefix, path = fields[5:7]
        if prefix and path:
            m = r.match(prefix)
            if m:
                origin = path.rpartition(' ')[-1]
                address, prefixlen = m.groups()
                prefixlen = int(prefixlen)
                if 0 < prefixlen <= 24:
                    c[(address, prefixlen, origin)] += 1
    _, err = p.communicate()
    if p.returncode > 0:
        sys.stderr.write(err)
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


def organize_prefixes(prefixes_counter):
    prefixes = defaultdict(Counter)
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
    parser.add_argument('-f', '--files', required=True)
    parser.add_argument('-p', '--poolsize', type=int, default=-1)
    parser.add_argument('-o', '--output', type=FileType('w'), default='-')
    args = parser.parse_args()
    files = list(read_filenames(args.files))
    counter = parse_files(files, args.poolsize)
    prefixes = organize_prefixes(counter)
    write_prefixes(args.output, prefixes)


if __name__ == '__main__':
    main()
