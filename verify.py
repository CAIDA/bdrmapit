from typing import List, Dict, Set

import numpy as np
import pandas as pd

from graph.bdrmapit import Bdrmapit
from graph.interface import Interface
from updates_dict import Updates


class GroundTruth:
    def __init__(self, addr: str, asn: int, conn_asn: int):
        self.addr = addr
        self.asn = asn
        self.conn_asn = conn_asn

    def __repr__(self):
        return 'GT<addr={}, asn={}, conn_asn={}>'.format(self.addr, self.asn, self.conn_asn)


class Verify:
    def __init__(self, bdrmapit: Bdrmapit, datasets: Dict[str, Set[GroundTruth]]):
        self.bdrmapit: Bdrmapit = bdrmapit
        dfs = []
        for db, s in datasets.items():
            df = pd.DataFrame([gt.__dict__ for gt in s if gt.asn != 0 and gt.conn_asn != 0])
            df = df.rename(columns={'asn': 'tasn', 'conn_asn': 'tconn_asn'}).copy()
            df['torg'] = df.tasn.map(bdrmapit.as2org.__getitem__)
            df['tconn_org'] = df.tconn_asn.map(bdrmapit.as2org.__getitem__)
            df['dataset'] = db
            df['cone'] = df.tasn.map(bdrmapit.bgp.conesize.__getitem__)
            dfs.append(df)
        self.truth: pd.DataFrame = pd.concat(dfs, ignore_index=True)

    def interfaces(self):
        return self.truth.groupby('dataset').addr.unique().to_dict()

    def router_verify(self, row: pd.Series):
        torg = self.bdrmapit.as2org.name(row.tasn)
        org = self.bdrmapit.as2org.name(row.asn)
        return org == torg

    def link_verify(self, row: pd.Series):
        torg = self.bdrmapit.as2org.name(row.tasn)
        tconn_org = self.bdrmapit.as2org.name(row.tconn_asn)
        org = self.bdrmapit.as2org.name(row.asn)
        conn_org = self.bdrmapit.as2org.name(row.conn_asn)
        if torg != tconn_org:
            if org != conn_org:
                if (torg == org and tconn_org == conn_org) or (row.dataset == 'TWC' and torg == conn_org and tconn_org == org):
                # if torg == org and tconn_org == conn_org:
                    return 'TP'
                else:
                    return 'FP'
            else:
                return 'FN'
        else:
            if org == conn_org:
                if torg == tconn_org:
                    return 'TN'
                return 'TP'
            else:
                return 'FP'

    def verify(self, interface_dict: Dict[str, List[Interface]] = None, rupdates: Updates = None, iupdates: Updates = None, df: pd.DataFrame = None):
        if df is None:
            rows = []
            for db, interfaces in interface_dict.items():
                for interface in interfaces:
                    router = self.bdrmapit.graph.interface_router[interface]
                    rasn, rorg, rutype = self.bdrmapit.lhupdates.get(router, rupdates[router])
                    if self.bdrmapit.as2org[interface.asn] == self.bdrmapit.as2org[rasn] or interface.asn == 0:
                        iasn, iorg, iutype = iupdates[interface]
                        if iasn == -1:
                            iasn, iorg, iutype = interface.asn, interface.org, -1
                    else:
                        iasn, iorg, iutype = interface.asn, interface.org, -2
                    rows.append([interface.address, router.name, iasn, str(iorg), iutype, rasn, str(rorg), rutype, db])
            df = pd.DataFrame(rows,
                              columns=['addr', 'router', 'conn_asn', 'conn_org', 'itype', 'asn', 'org', 'rtype', 'dataset'])
        df = self.truth.merge(df, left_on=['addr', 'dataset'], right_on=['addr', 'dataset'])
        df['rv'] = df.apply(self.router_verify, axis=1)
        df['lv'] = df.apply(self.link_verify, axis=1)
        return df


def comparison(g: pd.DataFrame):
    if 'router' in g:
        rg = g[~g.router.duplicated()]
        rv: pd.Series = rg.rv.value_counts()
        rt = rv.get(True, 0)
        rf = rv.get(False, 0)
        raccuracy = rt / (rt + rf)
    else:
        rt = rf = raccuracy = 0
    ig = g[~g.addr.duplicated()]
    lv: pd.Series = ig.lv.value_counts()
    itp = lv.get('TP', 0)
    ifp = lv.get('FP', 0)
    itn = lv.get('TN', 0)
    ifn = lv.get('FN', 0)
    ppv, recall = calcppv(ig)
    iaccuracy = itp / (itp + ifp + ifn)
    small = ig[ig.cone <= 0]
    large = ig[ig.cone > 0]
    ppvsmall, recallsmall = calcppv(small)
    ppvlarge, recalllarge = calcppv(large)
    index = ['rt', 'rf', 'racc', 'itp', 'ifp', 'itn', 'ifn', 'ppv', 'recall', 'iacc', 'ppvs', 'recalls', 'ppvl', 'recalll']
    return pd.Series([rt, rf, raccuracy, itp, ifp, itn, ifn, ppv, recall, iaccuracy, ppvsmall, recallsmall, ppvlarge, recalllarge], index=index)


def calcppv(ig: pd.DataFrame):
    if len(ig) > 0:
        lv: pd.Series = ig.lv.value_counts()
        itp = lv.get('TP', 0)
        ifp = lv.get('FP', 0)
        itn = lv.get('TN', 0)
        ifn = lv.get('FN', 0)
        if itp > 0 or ifp > 0:
            ppv = itp / (itp + ifp)
        else:
            ppv = np.nan
        if itp > 0 or ifn > 0:
            recall = itp / (itp + ifn)
        else:
            recall = np.nan
        return ppv, recall
    return np.nan, np.nan
