import socket
import struct

from graph.router cimport Router


cdef class Interface(Router):

    def __init__(self, str address, int asn, str org, long num = -1):
        super().__init__(address)
        self.address = address
        self.asn = asn
        self.org = org
        self.rid = ''
        if num >= 0:
            self.num = num
        else:
            self.num = struct.unpack("!L", socket.inet_aton(self.address))[0]

    def __repr__(self):
        return 'Interface<{}>'.format(self.name)
