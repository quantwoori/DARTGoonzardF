from quantiwise.data.CVT_rawdat import csv_to_dict


class QTWifrs:
    dbname = 'WFNC2DB'
    request_value = csv_to_dict('ifrs')
