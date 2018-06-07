import re
from typing import Set

import pandas as pd

from nzix import html
from utils.utils import otherside
from verify import GroundTruth

mappings = {
    "AUT": 24398,   # Auckland University of Technology
    "AWA": 55485,   # TE WHARE WANANGA O AWANUIARANGI
    "BOP": 55524,   # Bay of Plenty Polytechnic
    # "CAL": 132264, # Callahan Innovation
    "CAL": 133857,  # Callahan Innovation
    "CON": 0,       # Conferences
    "CPT": 45138,   # Christchurch Polytechnic Institute
    "DIO": 55887,   # Diocesan School for Girls
    "EIT": 55702,   # Eastern Institute of Technology
    "EPL": 0,       # Education Payroll
    "ESR": 38626,   # ESR
    "GNS": 4667,    # GNS Science
    "INS": 17705,   # inspire net
    "KRS": 38827,   # Kristin School
    "LCN": 38319,   # Lincoln University
    "LIZ": 132096,  # Land Information New Zealand
    "LND": 38479,   # Landcare Research
    "LWR": 45267,   # Lightwire
    "MAS": 9433,    # Massey University
    "MIT": 10110,   # Manukau Institute of Technology
    "MOE": 24318,   # Ministry of Education
    "NEI": 132450,  # New Era IT Ltd
    "NGL": 132709,  # New Zealand Genomics Limited
    "NLZ": 9338,    # National Library of New Zealand
    "NWA": 18378,   # National Institute of Water and Atmosphere
    "OTP": 38906,   # Otago Polytechnic
    "PFR": 17542,   # Plant & Food == merged Crop & Food and Hort Research
    "RNZ": 45131,   # REANNZ Head Office
    "SDN": 0,       # Where I traced from.
    "SIT": 55747,   # Southern Institute of Technology
    "SRL": 38140,   # Scion Research Limited == NZ Forest Research
    "UCL": 55516,   # Universal College of Learning
    "UOA": 9431,    # University of Auckland
    "UOC": 9432,    # University of Canterbury
    "UOO": 38305,   # University of Otago
    "UOW": 681,     # University of Waikato
    "UTC": 133928,  # Unitec Institute of Technology
    "VUW": 23905,   # Victoria University of Wellington
    "WCP": 0,       # WCP and WEL -> WSQ
    "WEL": 45787,   # WCP and WEL -> WSQ
    "WSQ": 0,
    "WIT": 55746,   # Western Institute of Technology
    "WYN": 64054,   # Wynyard Group
    "aarnet-grt": 7575,  # AARnet
    "aarnet-grt-jeopardy": 0,
    "aarnet-sxt": 7575,  # AARnet
    "akamai": 20940,
    "ape-route-servers": 0,
    "bgp-monitoring": 0,
    # "catalyst": 24226,  # Catalyst.Net Ltd
    "chix-route-servers": 0,
    # "cloudflare": 13335,
    "connected-health": 0,
    "connected-health-member": 0,
    "fx-domestic": 9503,
    "ggc": 0,
    "inverloop-cluster": 0,
    "inverloop-cluster-v6": 0,
    # "microsoft": 8075,
    # "n4l": 133420,  #Network for Learning
    "netflix": 2906,
    "new-plymouth-boys": 0,
    # "other-shaper": 7575,  # AARnet
    # "other-shaper": 38022,  # I don't think the original mapping was correct
    # "pch": 715,
    # "pch-stats": 715,
    "reannz": 0,
    "shaper": 0,
    "tein": 24490,  # Trans-Eurasia Inforamtion Network
    "vuw-tinkering": 132003,  # VUW Network Research Lab
    "wix-route-servers": 0,
    # manual
    # "131.203.111.126": 9503,  # on APE peering, alias of 192.203.154.95
    # "202.61.2.17": 18119,     # on APE peering, alias of 192.203.154.117
    "202.20.104.2": 38473,    # static routed.
    # "103.23.17.22": 132917,   # on APE peering, yellow pages.
}


def parse(filename, use_otherside=False):
    rows = []
    reannz = 38022
    regex = re.compile(r'(.+?) (.+?) (.+?) (\d+\.\d+\.\d+\.\d+)')
    seen = set()
    with open(filename) as f:
        for line in f:
            m = regex.match(line)
            if m:
                router, network, label, address = m.groups()
                # asn = ip2as[address]
                asn = mappings.get(label, 0)
                conn_asn = reannz
                rows.append([address, asn, conn_asn, network, label])
                seen.add(address)
    if use_otherside:
        for address, asn, conn_asn, network, label in list(rows):
            # if asn >= 0:
            oside = otherside(address, prefixlen=31)
            if oside in seen:
                oside = otherside(address, prefixlen=30)
            if oside not in seen:
                rows.append([oside, conn_asn, asn, network, label])
                seen.add(oside)
    return pd.DataFrame(rows, columns=['Interface', 'ASN', 'ConnASN', 'Network', 'Label'])


def reannz_gt(filename, ixp=False, use_otherside=True) -> Set[GroundTruth]:
    gt = parse(filename, use_otherside=use_otherside)[['Interface', 'ASN', 'ConnASN']]
    if ixp:
        nzape = html('http://www.nzix.net/ape-peers.html')
        nzchix = html('http://www.wix.net.nz/chix-peers.html')
        nzwix = html('http://www.wix.net.nz/wix-peers.html')
        corrections = pd.DataFrame([
            ['192.203.154.49', 17412]
        ], columns=['Interface', 'ASN'])
        corrections['ConnASN'] = -1
        gt = pd.concat([gt, nzape, nzchix, nzwix, corrections], ignore_index=True).drop_duplicates(keep='last')
    return {GroundTruth(row.Interface, row.ASN, row.ConnASN) for row in gt.itertuples()}
    # return gt
