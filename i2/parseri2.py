#!/usr/bin/env python
import re
from argparse import ArgumentParser

import netaddr
from lxml import etree

from verify import GroundTruth

mappings = {
    'NLM via NIH': 393689,
    'BACKBONE': 0,
    'trcpsbb': 0,
    ' OOB': 0,
    ' VINI': 0,
    'Performance Assurance': 0,
    'bwctl': 0,
    'SPP Port': 0,
    'p2p': 0,
    'Perfsonar': 0,
    'Obs ': 0,
    'test.': 0,
    'Internet2': 0,
    'multicast': 0,
    'description': 0,
    'MANLAN Deepfield': -3,
    'Customer TransitRail': 11164,
    'Level3': 3356,
    'Amazon': 16509,
    'UOC': 160,
    'Microsoft': 8075,
    'University of Michigan': 36375,
    'Blue Jeans': 18541,
    'PSU': 3999,
    'Sun Corridor': 2900,
    'Code42': 62715,
    'Vidyo': 393462,
    'UMontana': 3807,
    'University of Wisconsin System': 3128,
    'USDA': 4152,
    'ESnet': 293,
    'ESNET': 293,
    'KanREN': 2495,
    'MANLAN Management': 0,
    'Logical Interface': 0,
    'Telepresence RE': 0,
    'MERIT': 237,
    'Singaren': 23855,
    'MCNC': 81,
    'RNP': 1916,
    'NEAAR': 396390,
    'OARnet': 600,
    'BOX.COM': 33011,
    'CIX New York': -1,
    'OneNet': 5078,
    'UW Science': 101,
    'Roche ': 2047,
    'Eqyptian National STI Network': 6879,
    'UEN': 210,
    'FLR ': 11096,
    'FRGP': 14041,
    'NCSA': 1224,
    'Biogenisis': -3,
    'TSG': -3,
    'CSTNET': 7497,
    'Eduroam': 11537,
    'KACST': 8895,
    'NREN': -3,
    'NIH': 3527,
    'Indiana Gigapop': 19782,
    'UNM': 3388,
    'AMPATH': 20080,
    'Ultralight': 32361,
    'ANKABUT': 47862,
    'Smithsonian': 25829,
    'UNL': 7896,
    'SERVICENOW': 16839,
    'Canarie': 6509,
    'Blackboard': 22556,
    'CEN': 22742,
    'CAAREN': 4901,
    'FranceTelecom': 5511,
    'redCLARA': 27750,
    'MOREnet': 2572,
    'GPN': 11317,
    'MREN': -3,
    'CIC Racklan': -3,
    'Otsuka': 29716,
    'MWT2': 160,
    'PAIX Palo Alto Multicast': -1,
    'Oracle': 7160
}


def parse(filename, ip2as):
    parser = etree.XMLParser(ns_clean=True, recover=True)
    root = etree.parse(filename, parser)
    ds = root.findall('.//{*}logical-interface/{*}description')
    rows = []
    seen = set()
    for d in ds:
        li = d.getparent()
        afn = li.find('.//{*}address-family-name')
        if afn.text == 'inet':
            af = afn.getparent()
            ifad = af.find('.//{*}ifa-destination')
            ifal = af.find('.//{*}ifa-local')
            if ifad is not None:
                desc = d.text
                net = ifad.text
                if net in seen:
                    continue
                seen.add(net)
                addr = ifal.text
                asn = 11537
                casn = ip2as[net]
                if casn < -1:
                    continue
                if casn == 11164:
                    asn = 11164
                if casn == 396450:
                    asn = 396450
                # if asn != 11537:
                #     continue
                if 'trcps' in desc:
                    continue
                plen = int(net.rpartition('/')[-1]) if '/' in net else 32
                if plen < 30 and casn != 11537 and casn != 11164 and casn != 396450:
                    continue
                m = re.search(r'\sAS:?\s?(\d+)', desc)
                if m:
                    casn = int(m.group(1))
                elif casn == asn or casn == 0:
                    if plen < 30 or plen == 32:
                        casn = asn
                    else:
                        casn = 0
                        for label, masn in mappings.items():
                            if label in desc:
                                if 'MOREnet' in desc:
                                    print(label, desc, masn)
                                if masn == 0:
                                    casn = asn
                                else:
                                    casn = masn
                                break
                if casn > 0 and asn > 0:
                    for host in netaddr.IPNetwork(net).iter_hosts():
                        if str(host) != addr:
                            if str(host) == '198.71.46.206':
                                print(addr, asn, casn)
                            rows.append(GroundTruth(str(host), casn, asn))
                if (casn == -1 or casn > 0) and (asn == -1 or asn > 0):
                    if addr == '198.71.46.206':
                        print(addr, asn, casn)
                    rows.append(GroundTruth(addr, asn, casn))
    return rows


def print_addrs(filename):
    parser = etree.XMLParser(ns_clean=True, recover=True)
    root = etree.parse(filename, parser)
    ds = root.findall('.//{*}logical-interface/{*}description')
    seen = set()
    for d in ds:
        li = d.getparent()
        afn = li.find('.//{*}address-family-name')
        if afn.text == 'inet':
            af = afn.getparent()
            ifad = af.find('.//{*}ifa-destination')
            ifal = af.find('.//{*}ifa-local')
            if ifad is not None:
                desc = d.text
                net = ifad.text
                if net.startswith('10.'):
                    continue
                if net in seen:
                    continue
                seen.add(net)
                for host in netaddr.IPNetwork(net).iter_hosts():
                    print(str(host))


def main():
    parser = ArgumentParser()
    parser.add_argument('filename')
    args = parser.parse_args()
    print_addrs(args.filename)


if __name__ == '__main__':
    main()
