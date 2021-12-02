from quantiwise.data.CVT_rawdat import csv_to_dict


class QTWStock:
    dbname = 'WFNS2DB'
    request_value = csv_to_dict('stock')