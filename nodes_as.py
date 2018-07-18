#!/usr/bin/env python
import sqlite3
import sys
from argparse import ArgumentParser, FileType

from utils.progress import Progress
from utils.utils import File2


def main():
    parser = ArgumentParser()
    parser.add_argument('-o', '--output', default='-')
    parser.add_argument('-v', '--verbose', action='store_true')
    parser.add_argument('db')
    args = parser.parse_args()
    query = 'SELECT nid, asn FROM node WHERE asn > -1 AND nid LIKE "N%"'
    con = sqlite3.connect(args.db)
    cur = con.cursor()
    result = cur.execute(query)
    if not args.verbose:
        Progress.set_output(False)
    pb = Progress(message='Reading from database', increment=1000000)
    with File2(args.output, read=False) as f:
        for nid, asn in pb.iterator(result):
            f.write('node.AS {} {} bdrmapIT\n'.format(nid, asn))
    cur.close()
    con.close()


if __name__ == '__main__':
    main()
