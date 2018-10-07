import sqlite3
import pandas as pd

from bgp.routing_table import RoutingTable
from utils.progress import Progress
from utils.utils import File2


def read_output(filename, as2org, addrs, dataset=None, parsed=None):
    alladdrs = {a for ifaces in addrs.values() for a in ifaces}
    rows = []
    if parsed is not None:
        alladdrs2 = set()
        con = sqlite3.connect(parsed)
        cur = con.cursor()
        pb = Progress(message='Reading addresses from {}'.format(filename), increment=1000000, callback=lambda: '{:,d}'.format(len(alladdrs2)))
        for hop1, hop2 in pb.iterator(cur.execute('select hop1, hop2 from adjacency')):
            if hop1 in alladdrs:
                alladdrs2.add(hop1)
            if hop2 in alladdrs:
                alladdrs2.add(hop2)
        cur.close()
        con.close()
    else:
        alladdrs2 = alladdrs
    con = sqlite3.connect(filename)
    cur = con.cursor()
    pb = Progress(message='Reading annotations from {}'.format(filename), increment=1000000, callback=lambda: '{:,d}'.format(len(rows)))
    for addr, asn, conn_asn, utype in pb.iterator(cur.execute('select addr, asn, conn_asn, utype from annotation WHERE asn != -1')):
        if addr in alladdrs2:
            for k, v in addrs.items():
                if addr in v:
                    rows.append([addr, asn, conn_asn, utype, k])
    cur.close()
    con.close()
    df = pd.DataFrame(rows, columns=['addr', 'asn', 'conn_asn', 'utype', 'dataset'])
    df['org'] = df.asn.map(as2org.__getitem__)
    df['conn_org'] = df.conn_asn.map(as2org.__getitem__)
    if dataset is not None:
        df['dataset'] = dataset
    return df


def nolasthop(filename, addrs):
    nolast = set()
    alladdrs = {a for ifaces in addrs.values() for a in ifaces}
    con = sqlite3.connect(filename)
    cur = con.cursor()
    pb = Progress(message='Reading addrs from {}'.format(filename), increment=1000000, callback=lambda: '{:,d}'.format(len(nolast)))
    for hop1, in pb.iterator(cur.execute('select hop1 from adjacency')):
        if hop1 in alladdrs:
            nolast.add(hop1)
    return nolast


def read_mapit(filename, as2org, addrs):
    alladdrs = {a for ifaces in addrs.values() for a in ifaces}
    rows = []
    df = pd.read_csv(filename)
    for row in df[df.Direct].itertuples():
        if row.Address in alladdrs and row.ASN != -1 and row.ConnASN != -1:
            if row.Direction:
                asn = row.ASN
                conn_asn = row.ConnASN
            else:
                asn = row.ConnASN
                conn_asn = row.ASN
            for k, v in addrs.items():
                if row.Address in v:
                    rows.append([row.Address, asn, conn_asn, k])

    df = pd.DataFrame(rows, columns=['addr', 'asn', 'conn_asn', 'dataset'])
    df['org'] = df.asn.map(as2org.__getitem__)
    df['conn_org'] = df.conn_asn.map(as2org.__getitem__)
    return df


def mapit_addresses(filename, addrs):
    alladdrs = {a for ifaces in addrs.values() for a in ifaces}
    seen = set()
    con = sqlite3.connect(filename)
    cur = con.cursor()
    try:
        pb = Progress(message='Reading addrs', increment=1000000, callback=lambda: 'Seen {:,d}'.format(len(seen)))
        for hop1, hop2 in pb.iterator(cur.execute('select hop1, hop2 from adjacency')):
            if hop1 in alladdrs:
                seen.add(hop1)
            if hop2 in alladdrs:
                seen.add(hop2)
    finally:
        cur.close()
        con.close()
    return seen


def mapit_add(results, seen, addrs, ip2as, as2org):
    rows = []
    found = set(results.addr)
    for addr in seen - found:
        asn = ip2as[addr]
        for k, v in addrs.items():
            if addr in v:
                rows.append([addr, asn, asn, k])
    df = pd.DataFrame(rows, columns=['addr', 'asn', 'conn_asn', 'dataset'])
    df['org'] = df.asn.map(as2org.__getitem__)
    df['conn_org'] = df.conn_asn.map(as2org.__getitem__)
    df = pd.concat([results, df])
    return df


def common(ax, xlabel=False, legend=True):
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.yaxis.grid(which='major', linestyle='--')
    if legend:
        ax.legend(title=None, loc='lower right')
    if not xlabel:
        ax.xaxis.label.set_visible(False)
    ax.set_axisbelow(True)


def read_bdrmap(filename, dataset):
    bdrmap = pd.read_sql('select * from annotation', sqlite3.connect(filename))
    bdrmap['dataset'] = dataset
    return bdrmap


def read_nodes(filename, increment=1000000):
    ip2as = RoutingTable.private()
    addrs = set()
    pb = Progress(message='Reading nodes', increment=increment, callback=lambda: 'Routers {:,d}'.format(len(addrs)))
    with File2(filename) as f:
        for line in pb.iterator(f):
            if line[0] != '#':
                _, n, *aliases = line.split()
                if len(aliases) > 1:
                    aliases = {addr for addr in aliases if ip2as[addr] >= 0}
                    if len(aliases) > 1:
                        addrs.update(aliases)
    return addrs
