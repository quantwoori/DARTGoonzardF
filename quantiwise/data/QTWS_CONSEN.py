from quantiwise.data.CVT_rawdat import csv_to_dict


class QTWConsensus:
    dbname = 'WFNR2DB'
    request_value = csv_to_dict('consensus')
