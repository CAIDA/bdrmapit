#!/usr/bin/env python
import math
import socket
import struct
from argparse import ArgumentParser
from collections import defaultdict

from utils.progress import Progress
from utils.utils import File2


def rirparse(filename):
    with File2(filename) as f:
        asns = defaultdict(set)
        for line in f:
            splits = line.split('|')
            if len(splits) < 8:
                continue
            _, _, rtype, start, value, _, allocated, extensions = splits
            if allocated == 'available':
                continue
            if not extensions:
                continue
            if rtype == 'asn':
                if allocated == 'reserved':
                    continue
                # asn = int(start)
                asn = start
                asns[extensions].add(asn)
            elif rtype == 'ipv4' or rtype == 'ipv6':
                if allocated == 'reserved':
                    # asn = {'-4'}
                    continue
                elif allocated == 'assigned' or allocated == 'allocated':
                    asn = asns.get(extensions)
                else:
                    continue
                if not asn:
                    continue
                for network, prefixlen in prefixes_iter(start, int(value)):
                    # asn = asns[extensions]
                    yield network, prefixlen, asn


def prefixlen_iter(num):
    while True:
        total_bits = math.log2(num)
        bits = int(total_bits)
        yield bits
        if total_bits == bits:
            break
        else:
            num -= 2**bits


def prefixes_iter(address, num):
    if '.' in address:
        fam = socket.AF_INET
    else:
        fam = socket.AF_INET6
    b = socket.inet_pton(fam, address)
    bitlen = len(b) * 8
    ipnum = int.from_bytes(b, 'big')
    for bits in prefixlen_iter(num):
        network = socket.inet_ntop(fam, ipnum.to_bytes(len(b), 'big'))
        prefixlen = bitlen - bits
        yield network, prefixlen
        ipnum += 2**bits


# def prefixes_iter(address, num):
#     ipnum = struct.unpack("!L", socket.inet_aton(address))[0]
#     for bits in prefixlen_iter(num):
#         network = socket.inet_ntoa(struct.pack('!L', ipnum))
#         prefixlen = 32 - bits
#         yield network, prefixlen
#         ipnum += 2**bits


def main():
    parser = ArgumentParser()
    parser.add_argument('-f', '--files')
    parser.add_argument('-r', '--rels')
    parser.add_argument('-c', '--cone')
    parser.add_argument('-o', '--output')
    args = parser.parse_args()
    with File2(args.files) as f:
        files = [line.strip() for line in f]
    prefixes = defaultdict(set)
    pb = Progress(len(files), 'Parsing RIR delegations', callback=lambda: 'Prefixes {:,d}'.format(len(prefixes)))
    for filename in pb.iterator(files):
        for network, prefixlen, asns in rirparse(filename):
            prefixes[network, prefixlen].update(asns)
    with open(args.output, 'w') as f:
        f.writelines('{}\t{}\t{}\n'.format(network, prefixlen, '_'.join(asns)) for (network, prefixlen), asns in prefixes.items())


if __name__ == '__main__':
    main()
