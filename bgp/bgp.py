from collections import Counter, defaultdict

import os.path
from glob import glob

import networkx as nx
import pandas as pd

from utils.utils import File2, decompresses_or_first

path = __file__.rpartition('/')[0]


def find_files(year, month, directory='caida/asrel'):
    date = '{}{:02d}01'.format(year, month)
    rels = glob(os.path.join(directory, '{}.as-rel.txt*'.format(date)))
    rel = decompresses_or_first(rels)
    cones = glob(os.path.join(directory, '{}.ppdc-ases.txt*'.format(date)))
    cone = decompresses_or_first(cones)
    return rel, cone


class BGP:
    def __init__(self, rels=None, cone=None, year=None, month=None, directory='caida/asrel', **kargs):
        if rels is None or cone is None:
            r, c = find_files(year, month, directory=directory)
            if rels is None:
                rels = r
            if cone is None:
                cone = c
        self.df = pd.read_table(rels, sep='|', comment='#', names=['Provider', 'Customer', 'Rel'])
        self._cone = {}
        self._conesize = {}
        with File2(cone) as f:
            for line in filter(lambda x: x[0] != '#', f):
                provider, *customers = line.split()
                provider = int(provider)
                customers = set(filter(provider.__ne__, map(int, customers)))
                self._cone[provider] = customers
                self._conesize[int(provider)] = len(customers)
        self._customer_rels = None
        self._customers = None
        self._providers = None
        self._relationships = None
        self._transit_rels = None
        self.graph = None
        self._num_customers = None
        self._peer = None

    def __contains__(self, item):
        return item in self._cone

    def _build_graph(self):
        if self.graph is None:
            self.graph = nx.DiGraph()
            self.graph.add_edges_from(self.df[['Provider', 'Customer']].itertuples(index=False))

    @property
    def num_customers(self):
        if self._num_customers is None:
            self._num_customers = Counter(self.df[self.df.Rel == -1].Provider)
        return self._num_customers

    def cone(self, asn):
        return self._cone.get(asn, set())

    def conesize(self, asn):
        return self._conesize.get(asn, 0)

    @property
    def customer_rels(self):
        if self._customer_rels is None:
            self._customer_rels = set(self.df[['Customer', 'Provider']].itertuples(index=False))
        return self._customer_rels

    @property
    def customers(self):
        if self._customers is None:
            self._customers = defaultdict(set)
            for row in self.df[self.df.Rel == -1].itertuples(index=False):
                self._customers[row.Provider].add(row.Customer)
        return self._customers

    @property
    def peer(self):
        if self._peer is None:
            self._peer = defaultdict(set)
            for row in self.df[self.df.Rel == 0].itertuples(index=False):
                self._peer[row.Provider].add(row.Customer)
                self._peer[row.Customer].add(row.Provider)
        return self._peer

    @property
    def providers(self):
        if self._providers is None:
            self._providers = defaultdict(set)
            for row in self.df[self.df.Rel == -1].itertuples(index=False):
                self._providers[row.Customer].add(row.Provider)
        return self._providers

    @property
    def relationships(self):
        if self._relationships is None:
            self._relationships = self.transit_rels | self.customer_rels
        return self._relationships

    @property
    def transit_rels(self):
        if self._transit_rels is None:
            self._transit_rels = set(self.df[['Provider', 'Customer']].itertuples(index=False))
        return self._transit_rels
