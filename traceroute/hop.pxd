cdef class Hop:
    """
        Represents a hop in the traceroute.

        :param addr: Hop address
        :param asn: IP-to-AS mapping of the hop address
        :param ttl: Probe TTL
        :param reply_ttl: TTL of the ICMP reply packet
        :param qttl: Quoted TTL of the probe packet in the reply packet
        :param icmp_type: The ICMP type of the reply packet (11 - TimeExceeded, 0 - EchoReply, 3 - DestinationUnreachable)
        """
    cdef public str addr
    cdef public int asn
    cdef public int ttl
    cdef public int reply_ttl
    cdef public int qttl
    cdef public bint private
    cdef public int icmp_type
    cdef public long num
