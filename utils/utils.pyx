import bz2
import gzip
import json
import logging
import pickle
from itertools import filterfalse
from socket import inet_ntoa, inet_aton
from struct import pack, unpack
from subprocess import Popen, PIPE

import numpy as np

log = logging.getLogger()


cdef class File2:
    def __init__(self, str filename, str compression='infer', bint read=True):
        self.filename = filename
        self.compression = infer_compression(filename) if compression == 'infer' else compression
        self.read = read

    def __enter__(self):
        if self.compression == 'gzip':
            self.f = gzip.open(self.filename, 'rt' if self.read else 'wt')
        elif self.compression == 'bzip2':
            self.f = bz2.open(self.filename, 'rt' if self.read else 'wt')
        else:
            self.f = open(self.filename, 'r' if self.read else 'w')
        return self.f

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.f.close()
        return False


cdef class DictDefault(dict):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.finalized = False

    cpdef void finalize(self) except *:
        self.finalized = True

    cpdef void unfinalize(self) except *:
        self.finalized = False


cdef class DictSet(DictDefault):
    def __missing__(self, key):
        cdef set newset = set()
        if not self.finalized:
            self[key] = newset
        return newset


cdef class DictInt(DictDefault):
    def __missing__(self, key):
        cdef int i = 0
        if not self.finalized:
            self[key] = i
        return i

cdef class DictList(DictDefault):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def __missing__(self, key):
        cdef list lst = []
        if not self.finalized:
            self[key] = lst
        return lst


cpdef str infer_compression(str filename, str default=None):
    ending = filename.rpartition('.')[2]
    if ending == 'gz':
        return 'gzip'
    elif ending == 'bz2':
        return 'bzip2'
    else:
        return default


cpdef list max_num(iterable, key=None):
    cdef list max_items = []
    cdef float max_value = float('-inf')
    cdef float value
    for item, value in zip(iterable, (map(key, iterable) if key else iterable)):
        if value > max_value:
            max_items = [item]
            max_value = value
        elif value == max_value:
            max_items.append(item)
    return max_items


cpdef tuple max2(iterable, key=None):
    first = None
    second = None
    first_value = np.NINF
    second_value = np.NINF
    for v in iterable:
        n = key(v) if key is not None else v
        if n > first_value:
            second = first
            second_value = first_value
            first = v
            first_value = n
        elif n > second_value:
            second = v
            second_value = n
    return first, first_value, second, second_value


def unique_everseen(iterable, key=None):
    seen = set()
    seen_add = seen.add
    if key is None:
        for element in filterfalse(seen.__contains__, iterable):
            seen_add(element)
            yield element
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                seen_add(k)
                yield element


def unique_single_element(iterable, key=None):
    seen = set()
    if key is None:
        for element in filterfalse(seen.__contains__, iterable):
            if len(seen) > 0:
                return False
            seen.add(element)
    else:
        for element in iterable:
            k = key(element)
            if k not in seen:
                if len(seen) > 0:
                    return False
                seen.add(k)
    return True


cpdef load_pickle(str filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


cpdef void save_pickle(str filename, obj) except *:
    with open(filename, 'wb') as f:
        pickle.dump(obj, f)


cpdef void save_json(str filename, obj) except *:
    with open(filename, 'w') as f:
        json.dump(obj, f)


def ls(fregex):
    p = Popen('/bin/bash -c "ls -1 {}"'.format(fregex), shell=True, universal_newlines=True, stdout=PIPE)
    for line in p.stdout:
        yield line.strip()


cpdef str otherside(str address, int prefixlen=-1, str network=None):
    if prefixlen < 0:
        prefixlen = int(network.partition('/')[2])
    ipnum = unpack("!L", inet_aton(address))[0]
    if prefixlen == 30:
        remainder = ipnum % 4
        if remainder == 1:
            oside = ipnum + 1
        else:
            oside = ipnum - 1
    elif prefixlen == 31:
        remainder = ipnum % 2
        if remainder == 0:
            oside = ipnum + 1
        else:
            oside = ipnum - 1
    else:
        raise Exception('{} is not 30 or 31'.format(prefixlen))
    return inet_ntoa(pack('!L', oside))


def read_filenames(filename):
    with open(filename) as f:
        for line in f:
            if not line.startswith('#'):
                line = line.strip()
                if line:
                    yield line


cpdef peek(s):
    for i in s:
        return i
