from graph.interface cimport Interface
from graph.router cimport Router
from utils.utils cimport DictList, DictSet

cdef int NEXTHOP, ECHO, MULTI, MULTIECHO

cdef class NameRouter(dict):
    cdef dict interface_dict

cdef class RouterInterfaces(DictList):
    pass

cdef class RouterDests(DictSet):
    cdef DictSet interface_dict

cdef class InterfaceRouter(dict):
    pass

cdef class HybridGraph:
    cdef public dict address_interface
    cdef public InterfaceRouter interface_router
    cdef public NameRouter name_router
    cdef public RouterInterfaces router_interfaces
    cdef public DictSet interface_dests
    cdef public RouterDests router_dests
    cdef public DictSet modified_interface_dests
    cdef public RouterDests modified_router_dests
    cdef public DictSet rnexthop
    cdef public DictSet recho
    cdef public DictSet rmulti
    cdef public DictSet inexthop
    cdef public DictSet imulti
    cdef public dict interface_succtype
    cdef public DictSet rnh_ases
    cdef public DictSet re_ases
    cdef public DictSet rm_ases
    cdef public rnh_interfaces
    cdef public re_interfaces
    cdef public rm_interfaces
    cdef public list routers
    cdef public list routers_succ
    cdef public list routers_nosucc
    cdef public list interfaces_pred

    cdef void add_interface(self, str address, int asn, str org) except *
    cdef Router add_router(self, str name)
    cdef void add_dest(self, str address, int asn) except *
    cdef int add_edge(self, str xaddr, str yaddr, int distance, int icmp_type) except -1
    cpdef set router_edge_dests(self, Router router, Interface subsequent, int rtype = *)
    cpdef HybridGraph copy(self)
    cpdef set iedges(self, Interface interface)
    cpdef set origin_ases(self, Router router, Interface interface, int priority)
    cpdef set redges(self, Router router, int priority)
    cpdef void group_interfaces(self, router, interfaces) except *
    cpdef void save(self, str filename) except *
    cpdef Interface interface(self, str address)
    cpdef Router router(self, str name)
    cpdef int num_interfaces(self) except -1
    cpdef int num_routers(self) except -1
