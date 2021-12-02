from util.Uapikey import get_token
from util.Umonkeypatch import no_ssl_verification

from dbms.DBmssql import MSSQL

from engine.SEdart1 import DARTSrchShareHolder

import msg.Mmsg as mesg
import msg.Mrequrl as reqsurl
import msg.DBstr as dbstr

import pandas as pd
import requests


class DARTQReport(DARTSrchShareHolder):
    def __init__(self):
        super().__init__()

    def search200(self, standard_year:int, standard_mth:int, rptcode='11014'):
        df = self.target200(standard_year, standard_mth)
        url = reqsurl.URL_RPTSTOCK
        token = get_token('dart', 'apikey')

        res = pd.DataFrame(None)
        with no_ssl_verification():
            i = 0
            for stk, crp, n in zip(df.stock_code, df.corp_code, df.corp_name):
                i += 1
                param = {'crtfc_key': token,
                         'corp_code': crp,
                         'bsns_year': str(standard_year),
                         'reprt_code': rptcode}
                req = requests.get(url, params=param)
                r = req.json()

                for info in r['list']:
                    res = res.append(
                        pd.DataFrame([info])
                    )
                if i == 12:  # Test
                    return res


if __name__ == '__main__':
    test = DARTQReport()
    a = test.search200(2021, 11)