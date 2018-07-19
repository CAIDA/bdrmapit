#!/usr/bin/env python
import sqlite3
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter

from utils.progress import Progress
from utils.utils import File2


def main():
    parser = ArgumentParser(description='Creates CAIDA nodes file from bdrmapIT sqlite output.', formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument('-o', '--output', default='-', help='Output file (- for stdout). Filenames ending in .bz2/gz will be compressed as bzip2/gzip.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show progress in millions of database rows.')
    parser.add_argument('db', help='The sqlite database file output by bdrmapIT.')
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
