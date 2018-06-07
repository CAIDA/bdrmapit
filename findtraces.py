#!/usr/bin/env python
import json
from argparse import ArgumentParser, FileType
from multiprocessing.pool import Pool
from typing import Set

from traceroute.warts import Warts
from utils.progress import Progress
from utils.utils import File2

addrs: Set[str] = set()


def find(filename):
    traces = []
    with Warts(filename, json=False) as f:
        for l in f:
            try:
                j = json.loads(l)
                hops = j.get('hops')
                if hops:
                    for hop in hops:
                        addr = hop['addr']
                        if addr in addrs:
                            traces.append(l)
                            # print(addr)
                            break
            except json.JSONDecodeError:
                pass
    return traces


def main():
    parser = ArgumentParser()
    parser.add_argument('-a', '--addrs')
    parser.add_argument('-w', '--warts')
    parser.add_argument('-o', '--output', type=FileType('w'))
    args = parser.parse_args()
    with File2(args.addrs) as f:
        addrs.update(l.strip() for l in f)
        print('Addrs: {:,d}'.format(len(addrs)))
    with File2(args.warts) as f:
        files = [l.strip() for l in f]
    found = 0
    pb = Progress(len(files), 'Reading files', callback=lambda: '{:,d}'.format(found))
    # for newtraces in pb.iterator(map(find, files)):
    #     for trace in newtraces:
    #         print(trace)
    #         found += 1
    with Pool(35) as pool:
        for newtraces in pb.iterator(pool.imap_unordered(find, files)):
            for trace in newtraces:
                args.output.write(trace)
                found += 1


if __name__ == '__main__':
    main()