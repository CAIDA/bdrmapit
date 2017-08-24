import signal
from argparse import ArgumentParser
from collections import namedtuple
from datetime import timedelta, datetime

import os.path
import subprocess

import logging

import requests
from dateutil.parser import parse

from progress import Progress
from utils import setup_parallel, create_cluster, stop_cluster

log = logging.getLogger()
Info = namedtuple('Info', ['team', 'date'])

outputdir = None

routeviews1 = ['route-views.chicago', 'route-views.eqix', 'route-views.isc', 'route-views.jinx', 'route-views.kixp', 'route-views.linx', 'route-views.nwax', 'route-views.perth', 'route-views.saopaulo', 'route-views.sfmix', 'route-views.sg', 'route-views.soxrs', 'route-views.sydney', 'route-views.telxatl', 'route-views.wide', 'route-views4', 'route-views3']


def exists(url):
    response = requests.head(url)
    return response.status_code == 200


class Popen2:
    def __init__(self, max_num):
        self.max_num = max_num
        self.ps = []

    def run(self, cmd, stderr=subprocess.DEVNULL, **kargs):
        while len(self.ps) >= self.max_num:
            for i, (p, c) in enumerate(self.ps):
                p.poll()
                if p.returncode is not None:
                    o = self.ps.pop(i)
                    print('Done: {}'.format(c))
                    break
        print(cmd)
        p = subprocess.Popen(cmd, stderr=stderr, **kargs)
        self.ps.append((p, cmd))

    def wait(self):
        for (p, c) in self.ps:
            p.wait()
            print('Done: {}'.format(c))


def download(popen2, date, wget_args=None):
    if not wget_args:
        wget_args = ''
    wget_str = 'wget {}'.format(wget_args)
    output_file = '{}.{:02d}.{:02d}'.format(date.year, date.month, date.day)
    # popen2 = Popen2(10)
    for rv in routeviews1:
        url = 'http://archive.routeviews.org/{}/bgpdata/{}.{:02d}/RIBS/rib.{}{:02d}{:02d}.0000.bz2'.format(rv, date.year, date.month, date.year, date.month, date.day)
        if exists(url):
            cmd = '{} -O {} {}'.format(wget_str, os.path.join(outputdir, '{}.{}.bz2'.format(rv, output_file)), url)
            # print(cmd)
            popen2.run(cmd, shell=True)
    url = 'http://archive.routeviews.org/bgpdata/{}.{:02d}/RIBS/rib.{}{:02d}{:02d}.0000.bz2'.format(date.year, date.month, date.year, date.month, date.day)
    if exists(url):
        cmd = '{} -O {} {}'.format(wget_str, os.path.join(outputdir, 'route-views2.{}.bz2'.format(output_file)), url)
        # print(cmd)
        popen2.run(cmd, shell=True)
    url = 'http://archive.routeviews.org/oix-route-views/{}.{:02d}/oix-full-snapshot-{}-{:02d}-{:02d}-0000.bz2'.format(date.year, date.month, date.year, date.month, date.day)
    if exists(url):
        cmd = '{} -O {} {}'.format(wget_str, os.path.join(outputdir, 'oix.{}.bz2'.format(output_file)), url)
        # print(cmd)
        popen2.run(cmd, shell=True)
    for i in range(17):
        url = 'http://data.ris.ripe.net/rrc{:02d}/{}.{:02d}/bview.{}{:02d}{:02d}.0000.gz'.format(i, date.year, date.month, date.year, date.month, date.day)
        if exists(url):
            cmd = '{} -O {} {}'.format(wget_str, os.path.join(outputdir, 'rrc{:02d}.{}.gz'.format(i, output_file)), url)
            # print(cmd)
            popen2.run(cmd, shell=True)


def main():
    global today, outputdir
    parser = ArgumentParser()
    parser.add_argument('-p', '--pool', dest='pool', default=1, type=int)
    parser.add_argument('-s', '--start', dest='start')
    parser.add_argument('-e', '--end', dest='end')
    parser.add_argument('-d', '--dir', dest='dir', default='.')
    parser.add_argument('-n', '--nc', dest='nc')
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