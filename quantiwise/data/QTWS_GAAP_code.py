from quantiwise.data.CVT_rawdat import csv_to_dict


class QTWGaap:
    dbname = 'WFNC2DB'
    request_value = csv_to_dict('gaap')
