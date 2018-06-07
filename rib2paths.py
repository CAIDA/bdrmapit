import sys
from argparse import ArgumentParser, FileType
from multiprocessing.pool import Pool
from subprocess import Popen, PIPE

from utils.progress import Progress


def parse(filename):
    paths = set()
    cmd = 'bgpreader -d singlefile -o rib-file,{} -w 0,2147483648'.format(filename)
    p = Popen(cmd, shell=True, stdout=PIPE, universal_newlines=True)
    for line in p.stdout:
        fields = line.split('|')
        if len(fields) >= 10:
            path = fields[9]
            path = '|'.join(asn for asn in path.split() if asn[0] != '{')
            if path:
                paths.add(path)
    return paths


def main():
    parser = ArgumentParser()
    parser.add_argument('-p', '--poolsize', type=int, default=30)
    parser.add_argument('-o', '--output', type=FileType('w'), default=sys.stdout)
    parser.add_argument('files', type=FileType('r'))
    args = parser.parse_args()
    files = [line.strip() for line in args.files]
    paths = set()
    pb = Progress(len(files), 'Extracting paths', callback=lambda: '{:,d}'.format(len(paths)))
    with Pool(args.poolsize) as pool:
        for newpaths in pb.iterator(pool.imap_unordered(parse, files)):
            paths.update(newpaths)
    print('Writing', file=sys.stderr)
    args.output.writelines(path + '\n' for path in paths)
    print('Cleaning up')


if __name__ == '__main__':
    main()