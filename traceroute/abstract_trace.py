from typing import List

from bgp.routing_table import RoutingTable
from traceroute.hop import Hop


FORWARD = 1
BACKWARD = 2
BOTH = 3


class AbstractTrace:

    def __init__(self, j, ip2as=None):
        self.j = j
        self.ip2as: RoutingTable = ip2as
        self.hops: List[Hop] = self._create_hopslist()
        self.allhops = list(self.hops)
        self.addrs = {h.addr for h in self.allhops if not h.private}
        self.prune_loops()
        self.remove_private()
        self.loop = False

    def __getitem__(self, item):
        return self.hops[item]

    @property
    def alladdrs(self):
        for hop in self.allhops:
            yield hop.addr

    def addrs_nums(self):
        for hop in self.hops:
            yield hop.addr, hop.num

    @property
    def dst(self) -> str:
        raise NotImplementedError

    @property
    def dst_asn(self) -> int:
        return self.ip2as[self.dst]

    def _create_hopslist(self):
        raise NotImplementedError

    def prune_loops(self):
        seen = set()
        previous = None
        end = 0
        for i, hop in enumerate(reversed(self.hops), 1):
            addr = hop.addr
            if addr != previous:
                if addr in seen:
                    end = i - 1
                previous = addr
                seen.add(addr)
        self.hops = self.hops[:len(self.hops) - end]
        self.loop = end > 0

    @property
    def stop_reason(self):
        raise NotImplementedError

    def suspicious_hop(self, dest_asn):
        for hop in self.hops[::-1]:
            asn = hop.asn
            if asn == dest_asn:
                return hop.ttl
        return self.hops[-1].ttl

    def remove_private(self):
        self.hops = [hop for hop in self.hops if not hop.private]
