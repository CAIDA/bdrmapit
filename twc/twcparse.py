import re
from typing import Set

import pandas as pd
from netaddr import IPNetwork, IPAddress

import bgp.routing_table as rt
from verify import GroundTruth

switch = {
    '107.14.16.189': 16625,
    '107.14.16.89': 16625,
    '66.109.7.17': 16625,
    '66.109.9.1': 16625,
}


def parse(filename, as2org):
    rows = []
    external = re.compile(r'[\d.]+/32 is in (([\d.]+)/\d+) on \S+ to .+?:+.+?:+(.+?):+(.+?):.*:(\d+\.\d+\.\d+\.\d+):.*')
    internal = re.compile(r'[\d.]+/32 is in (([\d.]+)/\d+) on BB - \S+ to \S+')
    with open(filename) as f:
        twcorg = as2org[7843]
        for line in f:
            e = external.match(line)
            i = internal.match(line)
            if e:
                network, address, conn_asn, name, _ = e.groups()
                if address == '24.27.236.30':
                    print(address, asn, conn_asn)
                if network == '198.32.118.83/24' or network == '198.32.176.83/24':
                    continue
                asn = 7843
                org = as2org[asn]
                if conn_asn == 'TBONE' and name == 'Akamai':
                    conn_asn = 20940
                # if network in switch:
                #     print(network)
                #     conn_asn = switch[network]
                conn_asn = int(conn_asn)
                if conn_asn == 65250 or conn_asn == 65251:
                    conn_asn = 2386
                elif conn_asn == 65270:
                    conn_asn = 40694
                conn_org = as2org[conn_asn]
            elif i:
                network, address = i.groups()
                asn = 7843
                conn_asn = 7843
                org = twcorg
                conn_org = twcorg
            else:
                continue
            net = IPNetwork(network)
            if net.prefixlen == 31 or (address != str(net.network) and address != str(net.broadcast)):
                if network == '66.109.7.37/30':
                    print(address, asn, conn_asn)
                addr = IPAddress(address)
                for host in net.iter_hosts():
                    if host == addr:
                        a, o, ca, co = asn, org, conn_asn, conn_org
                    else:
                        if str(host) == '24.27.236.31':
                            print(str(host), net.prefixlen, a, o, ca, co)
                        a, o, ca, co = conn_asn, conn_org, asn, org
                    rows.append([str(host), net.prefixlen, a, o, ca, co])
    return pd.DataFrame(rows, columns=['Interface', 'Prefix', 'ASN', 'Org', 'ConnASN', 'ConnOrg']).drop_duplicates(
        keep='first')


def twc_gt(filename) -> Set[GroundTruth]:
    rows = []
    asn = 7843
    twcexre = re.compile(r'(\d+\.\d+\.\d+\.\d+/\d+) is in ((\d+\.\d+\.\d+\.\d+)/(\d+)) on .* to .*:(\d+)::')
    twcakre = re.compile(r'(\d+\.\d+\.\d+\.\d+/\d+) is in ((\d+\.\d+\.\d+\.\d+)/(\d+)) on .* to .*:(TBONE)::')
    twcinre = re.compile(r'(\d+\.\d+\.\d+\.\d+/\d+) is in ((\d+\.\d+\.\d+\.\d+)/(\d+)) on BB')
    with open(filename) as f:
        for line in f:
            conn_asn = -1
            if line:
                e = twcexre.match(line)
                if e:
                    _, prefix, _, prefixlen, conn_asn = e.groups()
                    prefixlen = int(prefixlen)
                    conn_asn = int(conn_asn)
                else:
                    a = twcakre.match(line)
                    if a:
                        _, prefix, _, prefixlen, conn_asn = a.groups()
                        prefixlen = int(prefixlen)
                        conn_asn = 20940
                    else:
                        i = twcinre.match(line)
                        if i:
                            _, prefix, _, prefixlen = i.groups()
                            prefixlen = int(prefixlen)
                            conn_asn = 7843
                if rt.valid(conn_asn):
                    network = IPNetwork(prefix)
                    for host in network.iter_hosts():
                        rows.append([str(host), asn, conn_asn])
                    if prefixlen < 31:
                        rows.append([str(network.network), asn, conn_asn])
                        rows.append([str(network.broadcast), asn, conn_asn])
    df = pd.DataFrame(rows, columns=['Interface', 'ASN', 'ConnASN']).drop_duplicates(keep='first')
    # return df
    return {GroundTruth(row.Interface, row.ASN, row.ConnASN) for row in df.itertuples()}
