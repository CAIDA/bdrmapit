import re
import socket
from typing import List, Set

import numpy as np
import pandas as pd
import struct

import bgp.routing_table as rt
from utils.utils import File2
from verify import GroundTruth

asregex = re.compile(r'a?s?(\d+)')


def extract_asn(s):
    m = asregex.match(s)
    if m:
        return int(m.group(1))
    else:
        return np.nan


def otherside(address):
    num = struct.unpack('!L', socket.inet_aton(address))[0]
    other = num + 1 if num % 2 == 1 else num - 1
    return socket.inet_ntoa(struct.pack('!L', other))


def parse(filename, near=False) -> List[GroundTruth]:
    rows = []
    with File2(filename) as f:
        for line in f:
            if not line.startswith('='):
                external = re.match(r'.*,(\d+\.\d+\.\d+\.\d+),a?s?(\d+)', line)
                internal = re.match(r'.*(\d+\.\d+\.\d+\.\d+)\s+LINK FROM .* (\d+\.\d+\.\d+\.\d+)', line)
                if external:
                    address, asn = external.groups()
                    if near:
                        asn = 7018
                    else:
                        asn = int(asn)
                    oside = otherside(address)
                elif internal:
                    address, oside = internal.groups()
                    asn = 7018
                else:
                    print(line.strip())
                    continue
                conn_asn = 7018
                rows.append(GroundTruth(address, asn, conn_asn))
                # rows.append([address, asn, conn_asn])
    # return pd.DataFrame(rows, columns=['Interface', 'ASN', 'ConnASN'])
    return rows


def att_gt(*filenames) -> Set[GroundTruth]:
    attexre = re.compile(r'[a-z0-9]+,(\d+\.\d+\.\d+\.\d+),as(\d+)')
    attinre = re.compile(r'.* (\d+\.\d+\.\d+\.\d+)  LINK FROM .* (\d+\.\d+\.\d+\.\d+)')
    # attinre = re.compile(r'.* (\d+\.\d+\.\d+\.\d+)  LINK FROM .* (\d+\.\d+\.\d+\.\d+)')
    rows = set()
    conn_asn = 7018
    for filename in filenames:
        with open(filename) as f:
            for line in f:
                addresses = []
                asn = -1
                m = attexre.match(line)
                if m:
                    addresses = [m.group(1)]
                    asn = int(m.group(2))
                else:
                    i = attinre.match(line)
                    if i:
                        addresses = [i.group(1), i.group(2)]
                        asn = 7018
                for address in addresses:
                    rows.add(GroundTruth(address, asn, conn_asn))
                    # rows.append([address, asn, conn_asn])
    # df = pd.DataFrame(rows, columns=['Interface', 'ASN', 'ConnASN']).drop_duplicates(keep='first')
    # return df
    return rows
