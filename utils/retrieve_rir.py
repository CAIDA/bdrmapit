import signal
from argparse import ArgumentParser
from collections import namedtuple
from datetime import timedelta, datetime

from subprocess import Popen

import logging
from dateutil.parser import parse

from progress import Progress
from utils import setup_parallel, create_cluster, stop_cluster

log = logging.getLogger()
Info = namedtuple('Info', ['team', 'date'])

today = None
outputdir = None


def download(date):
    current_year = today.year
    wget_str = 'wget -N -P {}'.format(outputdir)
    ps = []
    p = Popen(('{} ftp://ftp.afrinic.net/pub/stats/afrinic/{}/delegated-afrinic-extended-{}{:02d}{:02d}').format(wget_str, date.year, date.year, date.month, date.day), shell=True)
    ps.append(p)
    p = Popen(('{} ftp://ftp.apnic.net/pub/stats/apnic/{}/delegated-apnic-extended-{}{:02d}{:02d}.gz').format(wget_str, date.year, date.year, date.month, date.day), shell=True)
    ps.append(p)
    p = Popen(
        ('{} ftp://ftp.ripe.net/pub/stats/ripencc/{}/delegated-ripencc-extended-{}{:02d}{:02d}.bz2'
         ).format(wget_str, date.year, date.year, date.month, date.day), shell=True)
    ps.append(p)
    p = Popen(
        ('{} ftp://ftp.lacnic.net/pub/stats/lacnic/delegated-lacnic-extended-{}{:02d}{:02d}'
         ).format(wget_str, date.year, date.month, date.day), shell=True)
    ps.append(p)
    if date.year == current_year:
        p = Popen(
            ('{} ftp://ftp.arin.net/pub/stats/arin/delegated-arin-extended-{}{:02d}{:02d}'
             ).format(wget_str, date.year, date.month, date.day), shell=True)
        ps.append(p)
    else:
        p = Popen(
            ('{} ftp://ftp.arin.net/pub/stats/arin/archive/{}/delegated-arin-extended-{}{:02d}{:02d}'
             ).format(wget_str, date.year, date.year, date.month, date.day), shell=True)
        ps.append(p)
    for p in ps:
        p.wait()


def main():
    global today, outputdir
    parser = ArgumentParser()
    parser.add_argument('-p', '--pool', dest='pool')
    parser.add_argument('-s', '--start', dest='start')
    parser.add_argument('-e', '--end', dest='end')
    parser.add_argument('-d', '--dir', dest='dir', default='.')
    args = parser.parse_args()

    log.setLevel(logging.INFO)

    today = datetime.today()
    outputdir = args.dir

    start = parse(args.start)
    print(start.day, start.month, start.year)
    if args.end:
        end = parse(args.end)
        print(end.day, end.month, end.year)
    else:
        end = start

    infos = [start + timedelta(i) for i in range((end - start).days + 1)]

    if args.pool:
        if args.stop:
            stop_cluster()
        p = create_cluster(args.pool)
        dv, lv = setup_parallel()
        with dv.sync_imports():
            import subprocess
    else:
        dv = None
        lv = None

    mapper = lv.map_async if lv else map
    pb = Progress(len(infos), 'Downloading traces', increment=1)
    for _ in pb.iterator(mapper(download, infos)):
        pass

    if args.pool:
        stop_cluster()

if __name__ == '__main__':
    main()