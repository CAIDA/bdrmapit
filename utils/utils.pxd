cdef class File2:
    cdef public str filename
    cdef public str compression
    cdef public bint read
    cdef public f
    cdef public bint override
    cdef public bint _close

cdef class DictDefault(dict):
    cdef public bint finalized
    cpdef void finalize(self) except *
    cpdef void unfinalize(self) except *

cdef class DictSet(DictDefault):
    pass

cdef class DictInt(DictDefault):
    pass

cdef class DictList(DictDefault):
    pass

cpdef str infer_compression(str filename, str default=*)
cpdef list max_num(iterable, key=*)
cpdef tuple max2(iterable, key=*)
cpdef load_pickle(str filename)
cpdef void save_pickle(str filename, obj) except *
cpdef void save_json(str filename, obj) except *
cpdef str otherside(str address, int prefixlen=*, str network=*)
cpdef peek(s)
