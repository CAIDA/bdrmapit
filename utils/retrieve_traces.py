import signal
from argparse import ArgumentParser
from collections import namedtuple
from datetime import timedelta

import subprocess

import logging
from dateutil.parser import parse

from progress import Progress
from utils import setup_parallel, create_cluster, stop_cluster

log = logging.getLogger()
Info = namedtuple('Info', ['team', 'date'])


def download(info):
    team, date = info
    subprocess.run(('wget --user amarder@seas.upenn.edu --password abjl6565 -r -l 1 -np -nH -nc --cut-dirs=2 '
                    '-P caida/traces/ -A "*{}{:02d}*.warts.gz" '
                    'https://topo-data.caida.org/team-probing/list-7.allpref24/team-{}/daily/{}/cycle-{}{:02d}{:02d}'
                    ).format(date.year, date.month, team, date.year, date.year, date.month, date.day), shell=True)


def main():
    parser = ArgumentParser()
    parser.add_argument('-p', '--pool', dest='pool')
    parser.add_argument('-s', '--stop', dest='stop', action='store_true')
    parser.add_argument('start')
    parser.add_argument('end')
    args = parser.parse_args()

    log.setLevel(logging.INFO)

    start = parse(args.start)
    end = parse(args.end)

    print(start.day, start.month, start.year)
    print(end.day, end.month, end.year)
    infos = [(team, start + timedelta(i)) for team in range(1, 4) for i in range((end - start).days + 1)]

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