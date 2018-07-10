#!/usr/bin/env python
import logging
import os
from argparse import ArgumentParser
from collections import namedtuple
from datetime import timedelta, datetime

from dateutil.parser import parse

from utils.progress import Progress
from utils.subprocess_pool import Popen2

log = logging.getLogger()
Info = namedtuple('Info', ['team', 'date'])

today = datetime.today()
outputdir = None


def download(popen2, date, wget_args=None):
    if not wget_args:
        wget_args = ''
    wget_str = 'wget {}'.format(' '.join(wget_args))
    output_file = '{}.{:02d}.{:02d}'.format(date.year, date.month, date.day)
    url = 'ftp://ftp.ripe.net/pub/stats/ripencc/{}/delegated-ripencc-extended-{}{:02d}{:02d}.bz2'.format(date.year, date.year, date.month, date.day)
    cmd = '{} -O {} {}'.format(wget_str, os.path.join(outputdir, 'ripencc.{}.bz2'.format(output_file)), url)
    popen2.run(cmd, shell=True)
    url = 'ftp://ftp.apnic.net/pub/stats/apnic/{}/delegated-apnic-extended-{}{:02d}{:02d}.gz'.format(date.year, date.year, date.month, date.day)
    cmd = '{} -O {} {}'.format(wget_str, os.path.join(outputdir, 'apnic.{}.gz'.format(output_file)), url)
    popen2.run(cmd, shell=True)
    url = 'ftp://ftp.afrinic.net/pub/stats/afrinic/{}/delegated-afrinic-extended-{}{:02d}{:02d}'.format(date.year, date.year, date.month, date.day)
    cmd = '{} -O {} {}'.format(wget_str, os.path.join(outputdir, 'afrinic.{}'.format(output_file)), url)
    popen2.run(cmd, shell=True)
    url = 'ftp://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-{}{:02d}{:02d}'.format(date.year, date.month, date.day)
    cmd = '{} -O {} {}'.format(wget_str, os.path.join(outputdir, 'lacnic.{}'.format(output_file)), url)
    popen2.run(cmd, shell=True)
    if date.year == today.year:
        url = 'ftp://ftp.arin.net/pub/stats/arin/delegated-arin-extended-{}{:02d}{:02d}'.format(date.year, date.month, date.day)
    else:
        url = 'ftp://ftp.arin.net/pub/stats/arin/archive/{}/delegated-arin-extended-{}{:02d}{:02d}'.format(date.year, date.year, date.month, date.day)
    cmd = '{} -O {} {}'.format(wget_str, os.path.join(outputdir, 'arin.{}'.format(output_file)), url)
    popen2.run(cmd, shell=True)


def main():
    global outputdir
    parser = ArgumentParser()
    parser.add_argument('-p', '--pool', dest='pool', default=1, type=int)
    parser.add_argument('-s', '--start', dest='start')
    parser.add_argument('-e', '--end', dest='end')
    parser.add_argument('-d', '--dir', dest='dir', default='.')
    parser.add_argument('-n', '--nc', dest='nc', action='store_true')
    args = parser.parse_args()

    log.setLevel(logging.INFO)

    outputdir = args.dir
    os.makedirs(outputdir, exist_ok=True)

    start = parse(args.start)
    print(start.day, start.month, start.year)
    if args.end:
        end = parse(args.end)
        print(end.day, end.month, end.year)
    else:
        end = start

    infos = [start + timedelta(i) for i in range((end - start).days + 1)]

    wget_args = []
    if args.nc:
        wget_args.append('-nc')

    popen2 = Popen2(args.pool)

    pb = Progress(len(infos), 'Downloading traces', increment=1)
    for day in pb.iterator(infos):
        download(popen2, day, wget_args=wget_args)

    popen2.wait()


if __name__ == '__main__':
    main()
