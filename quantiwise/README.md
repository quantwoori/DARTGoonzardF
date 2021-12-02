# PyQuantiwise

Quantiwise is originally provided in Excel WorkSheet.
However, due to lack of multi-threads, it promptly shuts you down from using other excel function.
No need to say that querying can be very slow.

Because the whole data is stored in Woori-server(mssql) we can quickly query from the database directly,
instead of wasting your time on lousy excel, vba based program.

## Usage

* </b>Main File</b>: Pquantiwise
* </b>Auxiliary</b>: DBmssql, QTWS_CONSEN, QTWS_GAAP_code, QTWS_IFRS_code, QTWS_STOCK_code, token
* </b>Security</b>: seperate json file in './security'

### Initialize

```
quantiwise = PyQuantiwise()
```

### Stock data

```
test_stock = '005930'
s, e = '20210101', '20211010'
t1 = quantiwise.stk_data(test_stock, start_date=s, end_date=e, item='시가')
```

### Consensus data

```
cs = quantiwise.consen_data(test_stock, start_date=s, end_date=e, item='EPS')
```

uiw
