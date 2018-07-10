#!/usr/bin/env python
import csv
import sqlite3
from argparse import ArgumentParser, FileType

from utils.progress import Progress

parser = ArgumentParser()
parser.add_argument('input')
parser.add_argument('output', type=FileType('w'), default='-')
args = parser.parse_args()
writer = csv.writer(args.output, delimiter='\t')
con = sqlite3.connect(args.input)
cur = con.cursor()
print('reading adjs')
adjs = set(cur.execute('select hop1, hop2 from distance where distance > 0'))
used = 0
pb = Progress(message='Reading edges', increment=100000, callback=lambda: 'Used {:,d}'.format(used))
for row in pb.iterator(cur.execute('select hop1, hop2 from adjacency where distance = 1')):
    if row in adjs:
        writer.writerow(row)
        used += 1
print('cleaning up')