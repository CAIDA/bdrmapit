#!/usr/bin/env python
import os
import re
from argparse import ArgumentParser
from multiprocessing.pool import Pool

from utils.progress import Progress
from utils.utils import File2


outputdir = None


def create_nodes(filename):
    ip_re = re.compile(r'\d+\.\d+\.\d+\.\d+')

    nodes = []
    node = []
    with File2(filename) as f:
        for line in f:
            line = line.rstrip()
            if not line or line[0] == ' ':
                node = []
                nodes.append(node)
                if not line:
                    continue
            if line[0] == ' ':
                # print(ip_re.findall(line))
                nodes.append(ip_re.findall(line))
            else:
                m = ip_re.match(line)
                if m:
                    ip = m.group(0)
                    node.append(ip)
    nodes = {tuple(sorted(node)) for node in nodes}
    if outputdir is not None:
        newfile = os.path.join(outputdir, os.path.basename(filename))
    else:
        newfile = filename
    newfile = '{}.nodes'.format(newfile)
    with open(newfile, 'w') as f:
        for i, node in enumerate(nodes, 1):
            f.write('node N{}:  {}\n'.format(i, ' '.join(node)))


# def create_nodes2(filename):
#     owner_re = re.compile(r'owner')
#     ip_re = re.compile(r'(\d+\.\d+\.\d+\.\d+)')
# 
#     nodes = []
#     with File2(filename) as f:
#         for line in f:
#             if owner_re.match(line):
#                 node = []
#                 nodes.append(node)
#                 continue
#             m = ip_re.match(line)
#             if m:
#                 ip = m.group(1)
#                 node.append(ip)
# 
#     with open('{}.nodes'.format(filename), 'w') as f:
#         for i, node in enumerate(nodes, 1):
#             f.write('node N{}:  {}\n'.format(i, ' '.join(node)))


def main():
    global outputdir
    parser = ArgumentParser()
    parser.add_argument('-l', '--list', action='store_true', help='The file specified is a list of files.')
    parser.add_argument('-p', '--processes', default=1, type=int, help='Number of processes to use.')
    parser.add_argument('-o', '--outputdir', help='Directory for output file(s).')
    parser.add_argument('filename', help='bdrmap output file.')
    args = parser.parse_args()
    if args.outputdir:
        outputdir = args.outputdir
    if args.list:
        with File2(args.filename) as f:
            files = [l.strip() for l in f]
    else:
        files = [args.filename]
    pb = Progress(len(files), 'Extracting nodes', increment=1)
    if len(files) == 1 or args.processes == 1:
        for filename in pb.iterator(files):
            create_nodes(filename)
    else:
        with Pool(min(args.processes, len(files))) as pool:
            for _ in pb.iterator(pool.imap_unordered(create_nodes, files)):
                pass


if __name__ == '__main__':
    main()