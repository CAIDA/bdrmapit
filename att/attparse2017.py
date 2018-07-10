import re
from argparse import ArgumentParser

import netaddr

from verify import GroundTruth


def att_gt2017(filename):
    rows = []
    seen = set()
    with open(filename) as f:
        for line in f:
            addr, asn, conn_asn = None, None, None
            m = re.match(r'core:\s+(\d+\.\d+\.\d+\.\d+)', line)
            if m:
                # print(m)
                addr = m.group(1)
                asn, conn_asn = 7018, 7018
            # else:
            #     m = re.match(r'edge:\s+(\d+\.\d+\.\d+\.\d+)', line)
            #     if m:
            #         # print(m)
            #         addr = m.group(1)
            #         asn = conn_asn = 7018
            else:
                m = re.match(r' bgp: .*,(\d+.\d+.\d+.\d+),as(\d+)', line)
                if m:
                    # print(m)
                    addr = m.group(1)
                    asn = int(m.group(2))
                    conn_asn = 7018
            # if addr == '12.89.192.198':
            #     print(addr, asn, conn_asn)
            if addr and addr not in seen:
                seen.add(addr)
                # for host in netaddr.IPNetwork('{}/30'.format(addr)).iter_hosts():
                #     if str(host) != addr:
                #         rows.append(GroundTruth(str(host), conn_asn, asn))
                rows.append(GroundTruth(addr, asn, conn_asn))
    return rows


def main():
    parser = ArgumentParser()
    parser.add_argument('filename')
    args = parser.parse_args()
    rows = att_gt2017(args.filename)


if __name__ == '__main__':
    main()
