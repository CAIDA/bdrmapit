from graph.router cimport Router

cdef int ASN
cdef int ORG
cdef int UTYPE


cdef class Updates(dict):
    cdef public str name

    cpdef void add_update(self, Router node, int asn, str org, int utype) except *
    cpdef int asn(self, Router node) except -1
    cpdef void bdrmap_output(self, str filename, asn=*, str org=*, routers=*) except *
    cpdef links(self, interfaces, asn=*, org=*)
    cpdef str org(self, Router node)
    cpdef results(self, list interfaces, bint updates_only=*, list networks=*)