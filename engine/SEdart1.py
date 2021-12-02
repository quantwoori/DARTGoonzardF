from util.Uapikey import get_token
from util.Umonkeypatch import no_ssl_verification

from dbms.DBmssql import MSSQL

from engine.SEupdate import correct_code

import msg.Mmsg as mesg
import msg.Mrequrl as reqsurl
import msg.DBstr as dbstr

import pandas as pd
import requests


class DARTSrchShareHolder:
    def __init__(self, fileloc=r'../dat/corpcode_list.csv'):
        self.server = MSSQL().instance()
        self.server.login(
            id=get_token('id', 'sql'),
            pw=get_token('pw', 'sql')
        )

        self.dartcode = self.__dartcode(
            pd.read_csv(
                fileloc,
                encoding='utf-8',
                index_col=0
            )
        )

    @staticmethod
    def __dartcode(df:pd.DataFrame, chg=('stock_code', 'corp_code'),
                   retain='corp_name') -> pd.DataFrame:
        res = pd.DataFrame(None)
        res[chg[0]] = correct_code(df[chg[0]], 6)
        res[chg[1]] = correct_code(df[chg[1]], 8)
        res[retain] = df[retain]

        return res

    def _mnt_200(self, year:int, month:int, index_:str='ks200'):
        s = self.server.select_db(
            database=dbstr.SE1_DATABASE,
            schema=dbstr.SE1_SCHEMA,
            table=dbstr.SE1_TABLE,
            column=dbstr.SE1_COLUMN,
            condition=dbstr.SE1_COND.format(year, month, index_)
        )
        return pd.DataFrame(s, columns=dbstr.SE1_COLUMN)

    def target200(self, standard_year:int, standard_mth:int):
        k = self._mnt_200(
            year=standard_year,
            month=standard_mth
        )
        srch = k.merge(
            self.dartcode,
            how='inner',
            left_on='stk_no',
            right_on='stock_code'
        )

        return srch

    def search200(self, standard_year:int, standard_mth:int):
        df = self.target200(standard_year, standard_mth)
        url = reqsurl.URL_SHAREHLD
        token = get_token('dart', 'apikey')

        res = pd.DataFrame(None)
        with no_ssl_verification():
            i = 0
            for stk, crp, n in zip(df.stock_code, df.corp_code, df.corp_name):
                i += 1
                param = {'crtfc_key': token,
                         'corp_code': crp}

                req = requests.get(url, params=param)
                r = req.json()

                for info in r['list']:
                    res = res.append(
                        pd.DataFrame([info])
                    )
                if i == 12:

                    return res


    def search(self, standard_year, standard_mth):
        df = self.target200(standard_year, standard_mth)
        res = dict()
        for stk, crp, n in zip(df.stock_code, df.corp_code, df.corp_name):
            res[n] = (stk, crp)

        ...


if __name__ == '__main__':
    test = DARTSrchShareHolder()
    a = test.search200(2021, 11)