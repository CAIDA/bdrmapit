import re
from argparse import ArgumentParser

import netaddr

from verify import GroundTruth


def att_gt2018(*filenames):
    rows = []
    seen = set()
    for filename in filenames:
        with open(filename) as f:
            for line in f:
                addr, asn, conn_asn = None, None, None
                m = re.search(r'- [a-z0-9]+,(\d+\.\d+\.\d+\.\d+),as(\d+)', line)
                if m:
                    # print(m)
                    addr = m.group(1)
                    asn = int(m.group(2))
                    conn_asn = 7018
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
    rows = att_gt2018(args.filename)


if __name__ == '__main__':
    main()
