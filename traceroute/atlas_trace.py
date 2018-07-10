from traceroute.abstract_trace import AbstractTrace
from traceroute.hop import Hop


class AtlasTrace(AbstractTrace):

    def __init__(self, j, ip2as=None):
        super().__init__(j, ip2as=ip2as)

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
                            asn = self.ip2as[address]
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
    def dst(self):
        return self.j['dst_addr']