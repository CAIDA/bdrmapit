import os
import sqlite3
from argparse import ArgumentParser
from collections import Counter, defaultdict
from multiprocessing.pool import Pool

import pandas as pd

from utils.progress import Progress

gt = None


# def single(filename):
#     rows = []
#     con = sqlite3.connect(filename)
#     for addr, asn, conn_asn, rtype in con.execute('SELECT addr, asn, conn_asn, utype FROM annotation WHERE rtype != "m"'):
#         tasn, tcasn = gt[addr]
#         correct = {asn, conn_asn} == {tasn, tcasn}
#         if asn == conn_asn and tasn == tcasn:
#             c = 'same'
#         elif asn != conn_asn and tasn == tcasn:
#             c = 'inter'
#         elif asn == conn_asn and tasn != tcasn:
#             c = 'intra'
#         else:
#             c = 'diff'
#         rows.append([addr, asn, conn_asn, tasn, tcasn, correct, c])
#     return pd.DataFrame(rows, columns=['addr', 'asn', 'conn_asn', 'tasn', 'tconn_asn', 'correct', 'type'])

def single(filename):
    global gt
    base = os.path.basename(filename)
    counter = defaultdict(Counter)
    con = sqlite3.connect(filename)
    for addr, asn, conn_asn in con.execute('SELECT addr, asn, conn_asn FROM annotation WHERE rtype != "m" and utype >= 10'):
        tasn, tcasn = gt[addr]
        correct = {asn, conn_asn} == {tasn, tcasn}
        if asn == conn_asn and tasn == tcasn:
            c = 'same'
        elif asn != conn_asn and tasn == tcasn:
            c = 'inter'
        elif asn == conn_asn and tasn != tcasn:
            c = 'intra'
        else:
            c = 'diff'
        counter[correct][c] += 1
    con.close()
    alltrue = sum(counter[True].values())
    allfalse = sum(counter[False].values())
    intertrue = sum(counter[True].values()) - counter[True]['same']
    interfalse = sum(counter[False].values()) - counter[False]['same']
    allacc = alltrue / (alltrue + allfalse)
    interacc = intertrue / (intertrue + interfalse)
    return pd.Series({'alltrue': alltrue, 'allfalse': allfalse, 'intertrue': intertrue, 'interfalse': interfalse, 'allacc': allacc, 'interacc': interacc, 'file': base})


def naive(filename):
    counter = defaultdict(Counter)
    con = sqlite3.connect(filename)
    for addr, asn, conn_asn, distance in con.execute('SELECT addr, asn, conn_asn, distance FROM annotation WHERE distance = 1 OR asn = conn_asn'):
        tasn, tcasn = gt[addr]
        correct = {asn, conn_asn} == {tasn, tcasn}
        if asn == conn_asn and tasn == tcasn:
            c = 'same'
        elif asn != conn_asn and tasn == tcasn:
            c = 'inter'
        elif asn == conn_asn and tasn != tcasn:
            c = 'intra'
        else:
            c = 'diff'
        counter[c][correct] += 1
    return counter


def read_parallel(filenames, mappings, directory='.', method=None):
    global gt
    func = naive if method == 'naive' else single
    gt = mappings
    if isinstance(filenames, str):
        files = []
        with open(filenames) as f:
            for filename in f:
                base = os.path.basename(filename).rpartition('.')[0]
                db = os.path.join(directory, '{}.db'.format(base))
                files.append(db)
    else:
        files = filenames
    # files = files[10:20]
    counters = []
    pb = Progress(len(files), 'Reading')
    # with Pool(2) as pool:
    #     for counter in pb.iterator(pool.imap_unordered(func, files)):
    #         counters.append(counter)
    for counter in pb.iterator(map(func, files)):
        counters.append(counter)
    return pd.DataFrame(counters)
    # return pd.concat(dfs, ignore_index=True)


def main():
    parser = ArgumentParser()


if __name__ == '__main__':
    main()
