import socket
import struct


cdef class Hop:

    def __init__(self, addr, asn, ttl, reply_ttl, qttl=1, icmp_type=11):
        self.addr = addr                    # Hop address
        self.asn = asn                      # IP-to-AS mapping of the hop address
        self.ttl = ttl                      # Probe TTL
        self.reply_ttl = reply_ttl          # TTL of the ICMP reply packet
        self.qttl = qttl                    # Quoted TTL of the probe packet in the reply packet
        self.private = asn <= -2            # If the hop address is not globally unique
        self.icmp_type = icmp_type          # The ICMP type of the reply packet (11 - TimeExceeded, 0 - EchoReply, 3 - DestinationUnreachable)
        self.num = struct.unpack("!L", socket.inet_aton(self.addr))[0]

    def __repr__(self):
        return 'Hop(Addr={}, ASN={}, TTL={}, QTTL={})'.format(self.addr, self.asn, self.ttl, self.qttl)
