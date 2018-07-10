import math
import socket
import struct

from bgp.bgp import BGP
from utils.utils import File2


def rirparse(filename, bgp: BGP):
    with File2(filename) as f:
        asns = {}
        for line in f:
            splits = line.split('|')
            if len(splits) < 8:
                continue
            _, _, rtype, start, value, _, _, extensions = splits
            if not extensions:
                continue
            if rtype == 'asn':
                asn = int(start)
                current = asns.get(extensions)
                if not current:
                    asns[extensions] = asn
                elif bgp.conesize[current] >= bgp.conesize[asn]:
                    asns[extensions] = asn
            elif rtype == 'ipv4':
                asn = asns.get(extensions)
                if not asn:
                    continue
                for network, prefixlen in prefixes_iter(start, int(value)):
                    asn = asns[extensions]
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
    ipnum = struct.unpack("!L", socket.inet_aton(address))[0]
    for bits in prefixlen_iter(num):
        network = socket.inet_ntoa(struct.pack('!L', ipnum))
        prefixlen = 32 - bits
        yield network, prefixlen
        ipnum += 2**bits
