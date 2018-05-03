import json
from abc import ABC, abstractmethod
from collections import Counter
from enum import Enum
from typing import Optional

import cython

from bgp.routing_table import RoutingTable
from traceroute.warts import Warts
from utils.utils import File2
import numpy as np
import pandas as pd


ip2as: RoutingTable = None


class OutputType(Enum):
    warts = 1
    atlas = 2


class Set2(set):

    def __init__(self, use, *a, **k):
        super().__init__(*a, **k)
        self.use = use
        if not use:
            self.add = lambda x: None
            self.update = lambda x: None


class Counter2(Counter):

    @classmethod
    def fromkeys(cls, iterable, v=None):
        raise NotImplementedError()

    def __init__(self, use, *a, **k):
        super().__init__(*a, **k)
        self.use = use
        if not use:
            # self.__setitem__ = lambda x, y: None
            self.update = lambda x: None

    def __setitem__(self, key, value):
        if self.use:
            super().__setitem__(key, value)


class Hop:

    __slots__ = ['addr', 'asn', 'ttl', 'qttl', 'private', 'icmp_type', 'reply_ttl']

    def __init__(self, addr, asn, ttl, reply_ttl, qttl=1, icmp_type=11):
        """
        Represents a hop in the traceroute.

        :param addr: Hop address
        :param asn: IP-to-AS mapping of the hop address
        :param ttl: Probe TTL
        :param reply_ttl: TTL of the ICMP reply packet
        :param qttl: Quoted TTL of the probe packet in the reply packet
        :param icmp_type: The ICMP type of the reply packet (11 - TimeExceeded, 0 - EchoReply, 3 - DestinationUnreachable)
        """
        self.addr = addr                    # Hop address
        self.asn = asn                      # IP-to-AS mapping of the hop address
        self.ttl = ttl                      # Probe TTL
        self.reply_ttl = reply_ttl          # TTL of the ICMP reply packet
        self.qttl = qttl                    # Quoted TTL of the probe packet in the reply packet
        self.private = asn <= -2            # If the hop address is not globally unique
        self.icmp_type = icmp_type          # The ICMP type of the reply packet (11 - TimeExceeded, 0 - EchoReply, 3 - DestinationUnreachable)

    def __repr__(self):
        return 'Hop(Addr={}, ASN={}, TTL={}, QTTL={})'.format(self.addr, self.asn, self.ttl, self.qttl)


class Parser:

    def __init__(self, filename, output_type):
        self.filename = filename
        self.output_type = output_type

    def __iter__(self):
        if self.output_type == OutputType.warts:
            with Warts(self.filename, json=True) as f:
                for j in f:
                    if j['type'] == 'trace':
                        yield WartsTrace(j)
        elif self.output_type == OutputType.atlas:
            with File2(self.filename) as f:
                for j in map(json.loads, f):
                    yield AtlasTrace(j)


class AbstractTrace:

    def __init__(self, j, skip_processing=False):
        self.j = j
        if not skip_processing:
            self.hopslist = self._create_hopslist()
            self.hopslist = remove_loops(self.hopslist)
            self.hopslist = remove_private(self.hopslist)

    @property
    def addresses(self):
        raise NotImplementedError

    @property
    def dst(self) -> str:
        raise NotImplementedError

    @property
    def dst_asn(self) -> int:
        return ip2as[self.dst]

    @property
    def lasthop(self) -> Optional[Hop]:
        raise NotImplementedError

    def _create_hopslist(self):
        raise NotImplementedError

    @property
    def stop_reason(self):
        raise NotImplementedError

    def suspicious_hop(self, dest_asn):
        for hop in self.hopslist[::-1]:
            asn = hop.asn
            if asn == dest_asn:
                return hop.ttl
        return self.hopslist[-1].ttl


class AtlasTrace(AbstractTrace):
    def __bool__(self):
        return 'result' in self.j

    @property
    def stop_reason(self):
        return 'UNKNOWN'

    def _create_hopslist(self):
        newhops = []
        hops = self.j.get('result')
        if hops:
            h = None
            for hop in hops:
                h = None
                address = None
                replies = hop.get('result')
                if replies:
                    for result in replies:
                        curr_addr = result.get('from')
                        if curr_addr:
                            if address is not None and curr_addr != address:
                                h = None
                                break
                            address = result['from']
                            asn = ip2as[address]
                            ttl = hop['hop'] - 1
                            h = Hop(address, asn, ttl, result['ttl'])
                if h:
                    newhops.append(h)
            # Check if last hop has the same address as the destination.
            # If so, assume it's an Echo Reply (ICMP type 0).
            if h and h.addr == self.j['dst_addr']:
                h.icmp_type = 0
        return newhops

    @property
    def lasthop(self):
        hops = self.j.get('result')
        if hops:
            hop = hops[-1]
            h = None
            address = None
            replies = hop.get('result')
            if replies:
                for result in replies:
                    curr_addr = result.get('from')
                    if curr_addr:
                        if address is not None and curr_addr != address:
                            h = None
                            break
                        address = result['from']
                        asn = ip2as[address]
                        ttl = hop['hop'] - 1
                        h = Hop(address, asn, ttl, result['ttl'])
            if h:
                return h

    @property
    def dst(self):
        return self.j['dst_addr']

    @property
    def addresses(self):
        for hop in self.j.get('result', []):
            for result in hop.get('result', []):
                addr = result.get('from')
                if addr:
                    yield addr


class WartsTrace(AbstractTrace):

    @property
    def stop_reason(self):
        return self.j['stop_reason']

    @property
    def dst(self):
        return self.j['dst']

    def __bool__(self):
        return bool(self.j.get('hops'))

    def _create_hopslist(self):
        trace = np.full(self.j['hop_count'], fill_value=np.nan, dtype='object')
        hopslist = []
        hops = self.j.get('hops')
        if hops:
            for hop in hops:
                address = hop['addr']
                ttl = hop['probe_ttl'] - 1
                if pd.isnull(trace[ttl]):
                    asn = ip2as[address]
                    hop['asn'] = asn
                    trace[ttl] = address
                    h = Hop(address, asn, ttl, hop['reply_ttl'], qttl=hop.get('icmp_q_ttl', 1), icmp_type=hop['icmp_type'])
                    hopslist.append(h)
                elif trace[ttl] and trace[ttl] != address:
                    trace[ttl] = False
                    hopslist.pop()
        return hopslist

    @property
    def lasthop(self):
        hops = self.j.get('hops')
        if hops:
            hop = hops[-1]
            address = hop['addr']
            ttl = hop['probe_ttl'] - 1
            asn = ip2as[address]
            hop['asn'] = asn
            h = Hop(address, asn, ttl, hop['reply_ttl'], qttl=hop.get('icmp_q_ttl', 1), icmp_type=hop['icmp_type'])
            return h


    @property
    def addresses(self):
        for hop in self.j.get('hops'):
            yield hop['addr']


def remove_loops(hopslist):
    seen = set()
    previous = None
    end = 0
    for i, hop in enumerate(reversed(hopslist), 1):
        addr = hop.addr
        if addr != previous:
            if addr in seen:
                end = i - 1
            previous = addr
            seen.add(addr)
    return hopslist[:len(hopslist) - end]


def remove_private(hopslist):
    return [hop for hop in hopslist if not hop.private]
