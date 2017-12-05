import bz2
import gzip
import json
import logging
import pickle
import signal
import subprocess
from itertools import filterfalse
from multiprocessing import cpu_count
from socket import inet_ntoa, inet_aton
from struct import pack, unpack
from subprocess import Popen, PIPE
from sys import stderr
from time import sleep

import numpy as np

log = logging.getLogger()


class File2:
    def __init__(self, filename, compression='infer', read=True):
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


def decompresses_or_first(files):
    for filename in files:
        if not (filename.endswith('.bz2') or filename.endswith('.gz')):
            return filename
    return files[0]


def infer_compression(filename, default=None):
    ending = filename.rpartition('.')[2]
    if ending == 'gz':
        return 'gzip'
    elif ending == 'bz2':
        return 'bzip2'
    else:
        return default


def max_num(iterable, key=None):
    max_items = []
    max_value = np.NINF
    for item, value in zip(iterable, (map(key, iterable) if key else iterable)):
        if value > max_value:
            max_items = [item]
            max_value = value
        elif value == max_value:
            max_items.append(item)
    return max_items


def max2(iterable, key=None):
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


def load_pickle(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def save_pickle(filename, obj):
    with open(filename, 'wb') as f:
        pickle.dump(obj, f)


def save_json(filename, obj):
    with open(filename, 'w') as f:
        json.dump(obj, f)


def create_cluster(nodes, stop=False):
    if stop:
        stop_cluster()
    if nodes == 0:
        nodes = cpu_count() - 1
    p = Popen('ipcluster start -n {}'.format(nodes), universal_newlines=True, stderr=PIPE, shell=True)
    signal.signal(signal.SIGTERM, stop_cluster)
    for line in p.stderr:
        stderr.write(line)
        if 'Engines appear to have started successfully' in line:
            return p


def stop_cluster(verbose=True):
    p = subprocess.run('ipcluster stop', shell=True, universal_newlines=True, stderr=PIPE)
    if verbose:
        stderr.write(p.stderr)
    sleep(2)


def ls(fregex):
    p = Popen('/bin/bash -c "ls -1 {}"'.format(fregex), shell=True, universal_newlines=True, stdout=PIPE)
    for line in p.stdout:
        yield line.strip()


def otherside(address, prefixlen=None, network=None):
    if prefixlen is None:
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
