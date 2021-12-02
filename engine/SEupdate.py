from util.Uapikey import get_token
from util.Umonkeypatch import no_ssl_verification

import msg.Mmsg as mesg
import msg.Mrequrl as reqsurl

from typing import Iterable
from io import BytesIO
import pandas as pd
import requests
import zipfile
import xmltodict
import json


def unzip(zp_:zipfile.ZipFile, content='CORPCODE.xml', encode='utf-8'):
    x = zp_.read(content).decode(encode)
    xdict = xmltodict.parse(x)

    res = json.loads(json.dumps(xdict))
    print(mesg.DART_UPDATECORP_DONE)
    return res.get('result', {}).get('list')


def update_companycode():
    print(mesg.DART_UPDATECORP_PROC)
    token = get_token('dart', 'apikey')
    base = reqsurl.URL_CORPCODE

    with no_ssl_verification():
        res = requests.get(
            base, params={'crtfc_key': token}
        )
        dat = zipfile.ZipFile(
            BytesIO(res.content)
        )
    return unzip(dat)


def update():
    r = update_companycode()
    res = list()
    for i in r:
        if list(i.values())[2] is not None:
            res.append(i.values())
    t = pd.DataFrame(
        res,
        columns=[
            'corp_code',
            'corp_name',
            'stock_code',
            'modify_date'
        ]
    )
    t.to_csv(r'../dat/corpcode_list.csv', encoding='utf-8')
    print(mesg.DART_UPDATECORP_UPD)


def correct_code(value:Iterable, standard):
    result = list()
    for i in value:
        s = str(i)
        result.append(
            f"{'0' * (standard - len(s))}{s}"
        )
    return result


def corpcode_upd_main():
    update()
    company = pd.read_csv(
        '../dat/corpcode_list.csv',
        encoding='utf-8',
        index_col=0
    )[['corp_code', 'corp_name', 'stock_code', 'modify_date']]
    company['corp_code'] = correct_code(company['corp_code'], 8)
    company['stock_code'] = correct_code(company['stock_code'], 6)
    return company


if __name__ == '__main__':
    corpcode_upd_main()
