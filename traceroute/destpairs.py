import csv
from collections import defaultdict

import pandas as pd

from utils.progress import Progress
from utils.utils import File2


class Paths:
    def __init__(self, filename, objs, as2org, loop=False, same=False, chunksize=10000):
        self.allasns = defaultdict(dict)
        self.interface_asns = defaultdict(dict)
        pb = Progress(message='Reading dest pairs', increment=500000, multiplier=chunksize, callback=lambda: 'RouterASNs {:,d} InterfaceASNs {:,d}'.format(len(self.allasns), len(self.interface_asns)))
        for chunk in pb.iterator(pd.read_csv(filename, chunksize=chunksize)):
            for address, dest_asn, suspicious, second_last, last, stop_reason, stop_data in chunk.itertuples(index=False, name=None):
                if dest_asn > 0:
                    interface = objs.interfaces.get(address)
                    if interface:
                        org = as2org[dest_asn]
                        self.interface_asns[interface][org] = dest_asn
                        router = interface.router
                        self.allasns[router][org] = dest_asn
