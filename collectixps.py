#!/usr/bin/env python
import sqlite3
from argparse import ArgumentParser
import pandas as pd


def findixps(db):
    con = sqlite3.connect(db)
    df = pd.read_sql('SELECT prefix, i.id as id, i.name as name, ilp.created as created FROM peeringdb_ix i JOIN peeringdb_ixlan il ON (i.id = il.ix_id) JOIN peeringdb_ixlan_prefix ilp ON (il.id = ilp.ixlan_id)', con)
    df[['network', 'prefixlen']] = df.prefix.str.partition('/')[[0, 2]]
    return df[['network', 'prefixlen', 'id', 'name', 'created']]


def main():
    parser = ArgumentParser()
    parser.add_argument('-d', '--db', default='/home/amarder/.peeringdb/peeringdb.sqlite3', help='PeeringDB sqlite3 database file.')
    parser.add_argument('-o', '--output', required=True, help='Output filename.')
    args = parser.parse_args()
    df = findixps(args.db)
    df.to_csv(args.output, index=False)


if __name__ == '__main__':
    main()
