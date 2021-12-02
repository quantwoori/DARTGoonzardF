from dbms.DBmssql import MSSQL
from data.QTWS_STOCK_code import QTWStock
from data.QTWS_CONSEN import QTWConsensus
from util.Uapikey import get_token

import pandas as pd
import numpy as np

from typing import List, Tuple


class PyQuantiwise:
    qtwsto = QTWStock()
    qtwcns = QTWConsensus()

    def __init__(self):
        self.db = MSSQL().instance()
        self.db.login(
            id=get_token('id', 'sql'),
            pw=get_token('pw', 'sql')
        )

    def _get_schema(self, tablename:str, db:str) -> str:
        st = self.db.get_tablename(
            database=db
        )
        schema = st.loc[
            st.name == tablename
        ]['schema'].values[0]
        return schema

    # 주가 정보
    def stk_data(self, stock_code:str, start_date:str, end_date:str,
                 item:str='수정주가', tablename='TS_STK_DATA') -> pd.DataFrame:
        """
        주가 데이터
        """
        assert len(start_date) == 8 and len(end_date) == 8, "YYYYMMDD"
        # Set Condition
        item_code = self.qtwsto.request_value[item]
        c = f"STK_CD='{stock_code}' and ITEM_CD='{item_code}' and ITEM_TYP='S'"
        c = f"{c} and TRD_DT>='{start_date}' and TRD_DT<='{end_date}'"  # date info

        # Execute
        res, col_name = self._query_stk_idx(tn=tablename, condition=c)
        return pd.DataFrame(res, columns=col_name)

    def stock(self, stock_code:str, tablename='TS_STOCK') -> pd.DataFrame:
        """
        주식 개요
        """
        # Set Condition
        c = f"STK_CD='{stock_code}'"

        # Execute
        res, col_name = self._query_stk_idx(tn=tablename, condition=c)
        return pd.DataFrame(res, columns=col_name)

    def stk_issue(self, stock_code:str, start_date:str, end_date:str,
                  tablename:str='TS_STK_ISSUE') -> pd.DataFrame:
        """
        일자별 주식 정보
        """
        assert len(start_date) == 8 and len(end_date) == 8, "YYYYMMDD"
        # Set Condition
        c = f"STK_CD='{stock_code}'"
        c = f"{c} and TRD_DT>='{start_date}' and TRD_DT<='{end_date}'"  # date info

        # Execute
        res, col_name = self._query_stk_idx(tn=tablename, condition=c)
        return pd.DataFrame(res, columns=col_name)

    def stk_daily(self, stock_code:str, start_date:str, end_date:str,
                  tablename:str='TS_STK_DAILY') -> pd.DataFrame:
        """
        일별 주식 데이터
        """
        assert len(start_date) == 8 and len(end_date) == 8, "YYYYMMDD"
        # Set Condition
        c = f"STK_CD='{stock_code}'"
        c = f"{c} and TRD_DT>='{start_date}' and TRD_DT<='{end_date}'"  # date info

        # Execute
        res, col_name = self._query_stk_idx(tn=tablename, condition=c)
        return pd.DataFrame(res, columns=col_name)

    # 지수 정보
    def idx_daily(self, index_code:str, start_date:str, end_date:str,
                  tablename:str='TS_IDX_DAILY'):
        """
        일별 지수 데이터
        """
        assert len(start_date) == 8 and len(end_date) == 8, "YYYYMMDD"
        # Set condition
        c = f"SEC_CD='{index_code}'"
        c = f"{c} and TRD_DT>='{start_date}' and TRD_DT<='{end_date}'"

        # Execute
        res, col_name = self._query_stk_idx(tn=tablename, condition=c)
        return pd.DataFrame(res, columns=col_name)

    def idx_data(self, index_code:str, start_date:str, end_date:str,
                 item:str, tablename:str='TS_IDX_DATA') -> pd.DataFrame:
        """
        지수 데이터
        """
        # Set Condition
        item_code = self.qtwsto.request_value[item]
        c = f"SEC_CD='{index_code}' and ITEM_CD='{item_code}' and ITEM_TYP='I'"
        c = f"{c} and TRD_DT>='{start_date}' and TRD_DT<='{end_date}'"

        # Execute
        res, col_name = self._query_stk_idx(tn=tablename, condition=c)
        return pd.DataFrame(res, columns=col_name)

    # Market Information
    def market_date(self, tablename='TZ_DATE'):
        """
        일자 데이터. 직전 거래일 전전 거래일 ** 최근 거래일 **.
        날짜 맞출 때 유용.
        """
        res, col_name = self._query_stk_idx(tn=tablename, condition=None)
        return pd.DataFrame(res, columns=col_name)

    def market_sec(self, tablename='TC_SEC_REL'):
        """
        섹터별 주식. 편입시기
        """
        res, col_name = self._query_stk_idx(tn=tablename, condition=None)
        return pd.DataFrame(res, columns=col_name)

    # Consensus Information
    def consen_data(self, company:str, start_date:str, end_date:str,
                    item:str, tablename='TT_CMP_CNS_DATA'):
        assert len(start_date) == 8 and len(end_date) == 8, "YYYYMMDD"
        # Set Condition
        item_code = self.qtwcns.request_value[item]
        c = f"CMP_CD='{company}'"
        c = f"{c} and CNS_DT>='{start_date}' and CNS_DT<='{end_date}'"

        # Execute
        res, col_name = self._query_consensus(tn=tablename, condition=c)
        return pd.DataFrame(res, columns=col_name)

    def consen_analyst(self, tablename='TT_ANALYST'):
        res, col_name = self._query_consensus(tn=tablename, condition=None)
        return pd.DataFrame(res, columns=col_name)

    def consen_brokerage(self, tablename='TT_BROKERAGE'):
        res, col_name = self._query_consensus(tn=tablename, condition=None)
        return pd.DataFrame(res, columns=col_name)

    # Query Executioner
    def _query_stk_idx(self, tn:str, condition:str) -> (Tuple, List):
        database = self.qtwsto.dbname  # Stock and Index
        s = self._get_schema(tn, db=database)
        col = self.db.get_columns(
            database=database,
            table_name=tn,
            schema=s
        )
        if condition is not None:
            res = self.db.select_db(
                database=database,
                schema=s,
                table=tn,
                column=col,
                condition=condition
            )
        else:
            res = self.db.select_db(
                database=database,
                schema=s,
                table=tn,
                column=col,
            )
        return res, col

    def _query_consensus(self, tn:str, condition:str) -> (Tuple, List):
        database = self.qtwcns.dbname  # Consensus Data
        s = self._get_schema(tn, db=database)
        col = self.db.get_columns(
            database=database,
            table_name=tn,
            schema=s
        )
        if condition is not None:
            res = self.db.select_db(
                database=database,
                schema=s,
                table=tn,
                column=col,
                condition=condition
            )
        else:
            res = self.db.select_db(
                database=database,
                schema=s,
                table=tn,
                column=col,
            )
        return res, col

    def sql_decoder(self, value:str, encoder='ISO-8859-1', decoder='euc-kr') -> str:
        """
        Sometimes SQL spits out garbage decoded string - Korean.
        Reason is that SQL uses ISO-8859-1 decoder.
        Therefore encode -> decode it with euc-kr
        :param value: Garbage decoded string
        :return: Correctly decoded garbage
        """
        assert isinstance(value, str)
        return value.encode(encoder).decode(decoder)


if __name__ == '__main__':
    quantiwise = PyQuantiwise()

    test_stock = '000660'
    s, e = '20210503', '20211029'
    index_name = 'IKS001'
    # Unittest1
    t1 = quantiwise.stk_data(test_stock, start_date=s, end_date=e, item='종가')