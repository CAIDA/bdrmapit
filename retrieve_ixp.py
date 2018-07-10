import re
from argparse import ArgumentParser
from copy import copy
from typing import Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from utils.progress import Progress

ipv4_re = re.compile(r'\d+\.\d+\.\d+\.\d+/\d+')


def asns_iter(soup, label):
    for label in soup.find_all(string=label):
        num = label.find_next().text
        try:
            asn = int(num)
            if asn > 0:
                yield asn
        except ValueError:
            pass


def prefix_iter(soup, label):
    for label in soup.find_all(string=label):
        prefix = label.find_next().text
        if prefix and '.' in prefix:
            yield prefix


def extract_name(soup, label) -> Optional[str]:
    label = soup.find(string=label)
    if label:
        name = label.find_next().text
        return name.strip()


def expand_prefix(df: pd.DataFrame, label):
    rows = []
    index = df.index.name
    for row in df.reset_index().itertuples(index=False):
        d = row._asdict()
        if pd.notnull(d[label]):
            for prefix in ipv4_re.findall(d[label]):
                d2 = copy(d)
                d2[label] = prefix
                rows.append(d2)
    return pd.DataFrame(rows).set_index(index)


def rs_test(name):
    return name.str.contains('route server', case=False)


def name_test(names, series):
    return names.str.lower().isin(series.str.lower())


def extract_asns(ixp_id):
    asns = {}
    prefixes = {}
    r = requests.get('https://www.euro-ix.net/ixps/ixp-profile/?id={}'.format(ixp_id))
    soup = BeautifulSoup(r.text, 'lxml')
    name = extract_name(soup, 'Organization:')
    asns.update((asn, name) for asn in asns_iter(soup, 'AS Number:'))
    asns.update((asn, name) for asn in asns_iter(soup, 'Routeserver ASN'))
    prefixes.update((prefix, name) for prefix in prefix_iter(soup, 'Peering LAN Prefix'))
    return asns, prefixes


def main():
    parser = ArgumentParser()
    parser.add_argument('-o', '--output')
    parser.add_argument('--total', default=600, type=int)
    args = parser.parse_args()

    asns_dict = {}
    prefixes_dict = {}
    pb = Progress(args.total, 'Parsing IXP info', callback=lambda: 'ASNs {:,d} Prefixes {:,d}'.format(len(asns_dict), len(prefixes_dict)))
    for i in pb.iterator(range(args.total)):
        newasns, newprefixes = extract_asns(i)
        asns_dict.update(newasns)
        prefixes_dict.update(newprefixes)
    asns = pd.DataFrame(list(asns_dict.items()), columns=['asn', 'name']).set_index('asn').sort_index()
    prefixes = pd.DataFrame(list(prefixes_dict.items()), columns=['prefix', 'name']).set_index('prefix').sort_index()
    if len(prefixes) > 0:
        prefixes = expand_prefix(prefixes, 'prefix')
    ix = pd.io.json.json_normalize(requests.get('https://www.peeringdb.com/api/ix').json()['data']).set_index('id')
    ixlan = pd.io.json.json_normalize(requests.get('https://www.peeringdb.com/api/ixlan').json()['data']).set_index('id')
    prefs = pd.io.json.json_normalize(requests.get('https://www.peeringdb.com/api/ixpfx').json()['data']).set_index('id')
    prefs_expanded = expand_prefix(prefs, 'prefix')
    ixlan_pref = ixlan.merge(prefs_expanded, left_index=True, right_on='ixlan_id')
    ix_ixlan_pref = ix.merge(ixlan_pref[ixlan_pref.columns.difference(ix.columns)], left_index=True, right_on='ix_id')
    prefixes = pd.concat([prefixes, ix_ixlan_pref.set_index('prefix')[['name']]])
    nets = pd.io.json.json_normalize(requests.get('https://www.peeringdb.com/api/net').json()['data']).set_index('id')
    asns = pd.concat([asns, nets[rs_test(nets.name) | rs_test(nets.aka)].reset_index().set_index('asn')[['name']]])
    asns = pd.concat([asns, nets[name_test(nets.name, ix.name) | name_test(nets.aka, ix.name)].reset_index().set_index('asn')[['name']]])
    subnet = pd.read_csv('https://prefix.pch.net/applications/ixpdir/download.php?s=subnet_active', skipinitialspace=True).set_index('exchange_point_id')
    pch_expanded = expand_prefix(subnet, 'subnet')
    prefixes = pd.concat([prefixes, pch_expanded.rename(columns={'subnet': 'prefix', 'short_name': 'name'}).reset_index().set_index('prefix')[['name']]])
    prefixes[~prefixes.index.duplicated(keep='first')].to_csv('ixp_prefixes.csv')
    asns[~asns.index.duplicated(keep='first')].to_csv('ixp_asns.csv')
    unique_asns = list(asns.index.drop_duplicates(keep='first'))
    with open('ixp_asns.txt', 'w') as f:
        f.writelines('{}\n'.format(asn) for asn in unique_asns)


if __name__ == '__main__':
    main()
