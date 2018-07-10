import pandas as pd


# def html(url, results, as2org):
#     ts = pd.read_html(url)[0]
#     ape = ts.rename(columns={'AS Number': 'ASN', 'Peering Details': 'PD'})
#     asns = ape.ASN.str.extract(r'AS(\d+)', expand=True).rename(columns={0: 'ASN'})
#     asns['ASN'] = asns.ASN.astype(int)
#     asns['Org'] = asns.ASN.map(as2org.__getitem__)
#     ips = ape.PD.str.extractall(r'IPv4 peer:\s+(\d+\.\d+\.\d+\.\d+)').reset_index(level=1, drop=True).rename(columns={0: 'Interface'})
#     apeixp = asns.merge(ips, left_index=True, right_index=True)
#     m = apeixp.merge(results, left_on='Interface', right_on='Interface')
#     return m


def html(url):
    ts = pd.read_html(url)[0]
    ape = ts.rename(columns={'AS Number': 'ASN', 'Peering Details': 'PD'})
    asns = ape.ASN.str.extract(r'AS(\d+)', expand=True).rename(columns={0: 'ASN'})
    asns['ASN'] = asns.ASN.astype(int)
    ips = ape.PD.str.extractall(r'IPv4 peer:\s+(\d+\.\d+\.\d+\.\d+)').reset_index(level=1, drop=True).rename(columns={0: 'Interface'})
    apeixp = asns.merge(ips, left_index=True, right_index=True)
    apeixp['ConnASN'] = -1
    return apeixp


def accuracy(df):
    return correct(df) / len(df)


def correct(df):
    return len(df[df.Org_x == df.Org_y])
