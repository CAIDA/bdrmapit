from collections import defaultdict

import pandas as pd

from utils.progress import Progress


class Paths:
    def __init__(self, filename, objs, as2org, loop=False, same=False, chunksize=10000):
        # self.interface_asns = defaultdict(dict)
        self.allasns = defaultdict(dict)
        # pb = Progress(message='Reading dest pairs', increment=100000, multiplier=chunksize, callback=lambda: 'InterfaceASNs {:,d} RouterASNs {:,d}'.format(len(self.interface_asns), len(self.allasns)))
        pb = Progress(message='Reading dest pairs', increment=500000, multiplier=chunksize, callback=lambda: 'RouterASNs {:,d}'.format(len(self.allasns)))
        for chunk in pb.iterator(pd.read_csv(filename, chunksize=chunksize)):
            for address, dest_asn, suspicious, second_last, last, stop_reason, stop_data in chunk.itertuples(index=False, name=None):
                if dest_asn > 0:
                    interface = objs.interfaces.get(address)
                    if interface:
                        org = as2org[dest_asn]
                        if stop_reason == 'COMPLETED':
                            if last:
                                if org == interface.org:
                                    continue
                        elif stop_reason == 'UNREACH':
                            if second_last or last:
                                if org == interface.org:
                                    continue
                        # self.interface_asns[interface][org] = dest_asn
                        router = interface.router
                        self.allasns[router][org] = dest_asn
