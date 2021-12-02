from qtwise.Pquantiwise import PyQuantiwise

quantiwise = PyQuantiwise()

test_stock = '005930'
s, e = '20210101', '20211010'
index_name = 'IKS001'
# Unittest1
t1 = quantiwise.stk_data(test_stock, start_date=s, end_date=e, item='시가')

# Unittest2
t2 = quantiwise.stock(test_stock)

# Unittest3
t3 = quantiwise.stk_issue(test_stock, start_date=s, end_date=e)

# Unittest4
t4 = quantiwise.stk_daily(test_stock, start_date=s, end_date=e)

# Unittest5
t5 = quantiwise.idx_daily(index_name, start_date=s, end_date=e)

# Unittest6
t6 = quantiwise.idx_data(index_name, start_date=s, end_date=e, item='시가')

# Unittest7
tz = quantiwise.market_date()

# Unittest8
tz2 = quantiwise.market_sec()

# Unittest0
cs = quantiwise.consen_data(test_stock, start_date=s, end_date=e, item='EPS')
