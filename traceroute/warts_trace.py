from traceroute.abstract_trace import AbstractTrace
from traceroute.hop import Hop


class WartsTrace(AbstractTrace):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def stop_reason(self):
        return self.j['stop_reason']

    @property
    def dst(self):
        return self.j['dst']

    def __bool__(self):
        return bool(self.j.get('hops'))

    def _create_hopslist(self):
        hopslist = []
        prev = None
        skip = 0
        hops = self.j.get('hops')
        if hops:
            for hop in hops:
                ttl = hop['probe_ttl']
                if ttl == skip:
                    continue
                addr = hop['addr']
                if prev and ttl == prev.ttl and addr != prev.addr:
                    skip = ttl
                    hopslist.pop(-1)
                    continue
                asn = self.ip2as[addr]
                prev = Hop(addr, asn, ttl, hop['reply_ttl'], qttl=hop.get('icmp_q_ttl', 1), icmp_type=hop['icmp_type'])
                hopslist.append(prev)
        return hopslist
