import os
import sqlite3
from argparse import ArgumentParser
from multiprocessing.pool import Pool

import pandas as pd

from utils.progress import Progress

gt = None


def single(filename):
    rows = []
    con = sqlite3.connect(filename)
    for addr, asn, conn_asn, rtype in con.execute('SELECT addr, asn, conn_asn, utype FROM annotation WHERE rtype != "m"'):
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
        rows.append([addr, asn, conn_asn, tasn, tcasn, correct, c])
    return pd.DataFrame(rows, columns=['addr', 'asn', 'conn_asn', 'tasn', 'tconn_asn', 'correct', 'type'])


def naive(filename):
    rows = []
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
        rows.append([addr, asn, conn_asn, tasn, tcasn, correct, c])
    return pd.DataFrame(rows, columns=['addr', 'asn', 'conn_asn', 'tasn', 'tconn_asn', 'correct', 'type'])


def read_parallel(filenames, mappings, directory='.', processes=30):
    global gt
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
    pb = Progress(len(files), 'Reading')
    with Pool(processes) as pool:
        return pd.concat(list(pb.iterator(pool.imap_unordered(single, files))), ignore_index=True)


def main():
    parser = ArgumentParser()


if __name__ == '__main__':
    main()
