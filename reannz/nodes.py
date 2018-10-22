from collections import namedtuple

from bgp.routing_table import RoutingTable
from findvrfs import otherside30, otherside31, pton

Info = namedtuple('Info', ['addr', 'router', 'pdesc', 'ldesc', 'type'])


mappings = {
    "AGR": 0,
    "AUT": 24398,   # Auckland University of Technology
    "AWA": 55485,   # TE WHARE WANANGA O AWANUIARANGI
    "AWM": 0,
    "BOP": 55524,   # Bay of Plenty Polytechnic
    "CAL": 133857,  # Callahan Innovation
    "CAT": 0,
    "CON": 0,       # Conferences
    "CPS": 0,
    "CPT": 45138,   # Christchurch Polytechnic Institute
    "DIO": 55887,   # Diocesan School for Girls
    "EIT": 55702,   # Eastern Institute of Technology
    "EPL": 0,       # Education Payroll
    "ESR": 38626,   # ESR
    "FSM": 0,
    "FXN": 9790,
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
    "TEC": 0,
    "UCL": 55516,   # Universal College of Learning
    "UOA": 9431,    # University of Auckland
    "UOC": 9432,    # University of Canterbury
    "UOO": 38305,   # University of Otago
    "UOW": 681,     # University of Waikato
    "UTC": 133928,  # Unitec Institute of Technology
    "VUW": 23905,   # Victoria University of Wellington
    "WCP": 45787,       # WCP and WEL -> WSQ
    "WEL": 45787,   # WCP and WEL -> WSQ
    "WSQ": 45787,
    "WIT": 55746,   # Western Institute of Technology
    "WYN": 64054,   # Wynyard Group
    "aarnet-grt": 7575,  # AARnet
    "aarnet-grt-jeopardy": 0,
    "aarnet-sxt": 7575,  # AARnet
    "akamai": 20940,
    "ape-route-servers": 0,
    "bgp-monitoring": 38299,
    "catalyst": 24226,  # Catalyst.Net Ltd
    "chix-route-servers": 0,
    "cloudflare": 13335,
    "connected-health": 0,
    "connected-health-member": 0,
    "eie-syd-apple": 714,
    "eie-syd-amazon": 16509,
    "eie-syd-dropbox": 19679,
    "eie-syd-twitter": 13414,
    "eie-syd-he.net": 6939,
    "facebook": 32934,
    "fastly": 54113,
    "fx-domestic": 9503,
    "ggc": 0,
    "google": 15169,
    "internal4": 38022,
    "internal6": 38022,
    "inverloop-cluster": 0,
    "inverloop-cluster-v6": 0,
    "microsoft": 8075,
    "n4l": 133420,  #Network for Learning
    "netflix": 2906,
    "new-plymouth-boys": 0,
    # "other-shaper": 7575,  # AARnet
    "other-shaper": 38022,  # I don't think the original mapping was correct
    "pch": 715,
    "pch-stats": 715,
    "reannz": 38022,
    "shaper": 38022,
    "tein": 24490,  # Trans-Eurasia Inforamtion Network
    "vuw-tinkering": 132003,  # VUW Network Research Lab
    "wix-route-servers": 0,
    # manual
    # "131.203.111.126": 9503,  # on APE peering, alias of 192.203.154.95
    # "202.61.2.17": 18119,     # on APE peering, alias of 192.203.154.117
    # "202.20.104.2": 38473,    # static routed.
    # "103.23.17.22": 132917,   # on APE peering, yellow pages.
}

ixp_routers = {
    "eie-syd",
    "ape",
    "wix",
    "chix",
    "akl-ix",
    "pacwave",
    "six"
}


def parse(filename, ip2as: RoutingTable, ixps=None):
    external = {}
    internal = {}
    seen = set()
    with open(filename) as f:
        for line in f:
            router, name, net, addr = line.split()
            if name in ixp_routers:
                continue
            asn = mappings[net]
            if asn == 38022:
                internal[addr] = Info(addr, router, name, net, 'internal')
            else:
                num = pton(addr)
                oside = None
                if ip2as[addr] in [7575] or net in ["CPS"]:
                    oside = otherside31(addr)
                elif num % 2 == 0:
                    oside = otherside30(addr)
                if not oside:
                    oside = otherside31(addr)
                if oside in external and addr not in seen:
                    print(oside, ip2as[oside], line.strip(), external[oside])
                external[oside] = Info(oside, router, name, net, 'external')
                seen.add(addr)
    addrs = {k: v for k, v in {**external, **internal}.items()}
    return addrs, external, internal


def verify(pairs, raddrs, addrs, prior, after):
    marked = {x for x, _ in pairs}
    tps = set()
    tns = set()
    fps = set()
    fns = set()
    mfns = set()
    for a in raddrs.keys() & addrs:
        info = raddrs[a]
        if a in marked:
            if info.type == 'internal':
                fps.add(a)
            else:
                tps.add(a)
        else:
            if info.type == 'internal':
                tns.add(a)
            elif 38022 not in prior[a]:
                tns.add(a)
            else:
                fns.add(a)
                if after[a]:
                    mfns.add(a)
    tp = len(tps)
    tn = len(tns)
    fp = len(fps)
    fn = len(fns)
    mfn = len(mfns)
    try:
        ppv = (tp / (tp + fp))
    except ZeroDivisionError:
        ppv = 0
    try:
        recall = (tp / (tp + fn))
    except ZeroDivisionError:
        recall = 0
    try:
        mrecall = (tp / (tp + mfn))
    except ZeroDivisionError:
        mrecall = 0
    print('TP {:,d} FP {:,d} FN {:,d} TN {:,d} PPV {:.1%} Recall {:.1%}'.format(tp, fp, fn, tn, ppv, recall))
    print('MFN {:,d} MRecall {:.1%}'.format(mfn, mrecall))
    return tps, tns, fps, fns, mfns
