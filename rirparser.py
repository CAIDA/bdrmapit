import socket
import struct

import math
from collections import defaultdict

import pandas as pd
from logging import getLogger

from bgp.bgp import BGP
from utils.utils import ls

log = getLogger()
dates = {'03-2016': (2016, 3, 28)}


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


def delegations(filename, bgp: BGP):
    df = pd.read_csv(filename, sep='|', names=['Registry', 'CC', 'Type', 'Start', 'Value', 'Date', 'Status', 'Extensions'], comment='#', dtype=object)
    asns = defaultdict(int)
    for row in df[pd.notnull(df.Extensions) & (df.Type == 'asn')].itertuples(index=False):
        if bgp.conesize[row.Start] > bgp.conesize[asns[row.Extensions]]:
            asns[row.Extensions] = row.Start
    # asns = {row.Extensions: int(row.Start) for row in df[pd.notnull(df.Extensions) & (df.Type == 'asn')].itertuples(index=False)}
    prefixes = [(network, prefixlen, asns[row.Extensions]) for row in df[(df.Extensions.isin(asns)) & (df.Type == 'ipv4')].itertuples(index=False) for network, prefixlen in prefixes_iter(row.Start, int(row.Value))]
    return prefixes


def all_prefixes(year=None, month=None, day=None, date=None):
    if date is not None:
        year, month, day = dates[date]
    rows = []
    for filename in ls('rir-delegations/delegated*{}{:02d}{:02d}*'.format(year, month, day)):
        log.debug(filename)
        rows.extend(delegations(filename))
    return rows
