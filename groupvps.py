#!/usr/bin/env python
from argparse import ArgumentParser

from as2org import AS2Org
from collections import defaultdict
import combine
from multiprocessing import Pool
import pandas as pd
import random
import re
import sqlite3
from utils.progress import Progress
from utils.utils import File2


outputdir = None


def combinep(args):
    monitor, fs = args
    combine.combinedbs(fs, '{}/monitors/{}.db'.format(outputdir, monitor))


def combine_groups(args):
    num, gid, group = args
    group = ['{}/monitors/{}.db'.format(outputdir, monitor) for monitor in group]
    combine.combinedbs(group, '{}/groups/{}/{}.db'.format(outputdir, num, gid))


def main():
    global outputdir
    parser = ArgumentParser()
    parser.add_argument('-a', '--as2org')
    parser.add_argument('-d', '--dbs')
    parser.add_argument('-e', '--exclude', type=int, nargs='*')
    parser.add_argument('-c', '--combine', action='store_true')
    parser.add_argument('-g', '--groups', action='store_true')
    parser.add_argument('-A', '--all', action='store_true')
    parser.add_argument('-o', '--outputdir', required=True)
    parser.add_argument('-p', '--poolsize', default=30, type=int)
    args = parser.parse_args()

    outputdir = args.outputdir

    as2org = AS2Org(args.as2org, include_potaroo=False)
    con = sqlite3.connect('monitors.db')
    df = pd.read_sql('SELECT monitor, asn, org AS name, start FROM monitor', con)
    con.close()
    df['org'] = df.asn.map(as2org.__getitem__)
    with File2(args.dbs) as f:
        files = [line.strip() for line in f]
        print('Files: {:,d}'.format(len(files)))
    mfiles = defaultdict(list)
    for filename in files:
        m = re.search(r'\.([-a-z0-9]+)\.warts.db', filename)
        mfiles[m.group(1)].append(filename)
    orgs = {as2org[asn] for asn in args.exclude} if args.exclude else set()
    for monitor in df[df.org.isin(orgs)].monitor:
        if monitor in mfiles:
            del mfiles[monitor]
    if args.combine:
        Progress.set_output(True)
        pb = Progress(len(mfiles), 'Combining')
        Progress.set_output(False)
        with Pool(args.poolsize) as p:
            for _ in pb.iterator(p.imap_unordered(combinep, mfiles.items())):
                pass
        Progress.set_output(True)
    if args.groups:
        groups = {i: [(i, j, random.sample(mfiles.keys(), i)) for j in range(5)] for i in range(20, 81, 20)}
        for i in range(20, 81, 20):
            Progress.set_output(True)
            pb = Progress(5, 'Combining {}'.format(i))
            Progress.set_output(False)
            with Pool(5) as p:
                for _ in pb.iterator(p.imap_unordered(combine_groups, groups[i])):
                    pass
        Progress.set_output(True)
    if args.all:
        print('Combining all')
        Progress.set_output(True)
        combine_groups(('all', 0, mfiles.keys()))


if __name__ == '__main__':
    main()