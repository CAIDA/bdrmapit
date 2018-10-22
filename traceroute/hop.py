import socket
import struct


class Hop:

    def __init__(self, addr, asn, ttl, reply_ttl, qttl=1, icmp_type=11, icmpext=None):
        if addr.startswith('::') and '.' in addr:
            addr = addr[2:]
        self.addr = addr                    # Hop address
        self.asn = asn                      # IP-to-AS mapping of the hop address
        self.ttl = ttl                      # Probe TTL
        self.reply_ttl = reply_ttl          # TTL of the ICMP reply packet
        self.qttl = qttl                    # Quoted TTL of the probe packet in the reply packet
        self.private = -100 < asn <= -2            # If the hop address is not globally unique
        self.icmp_type = icmp_type          # The ICMP type of the reply packet (11 - TimeExceeded, 0 - EchoReply, 3 - DestinationUnreachable)
        self.icmpext = icmpext

    def __repr__(self):
        return 'Hop(Addr={}, ASN={}, TTL={}, QTTL={} ITYPE={}, ICMPEXT={})'.format(self.addr, self.asn, self.ttl, self.qttl, self.icmp_type, bool(self.icmpext))
