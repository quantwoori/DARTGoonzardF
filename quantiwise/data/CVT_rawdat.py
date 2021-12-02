import pandas as pd


def __clean_value(value:str):
    return value.replace(' ', '')

def csv_to_dict(filename, base=r'./rawdat/'):
    dat = pd.read_csv(
        f"{base}{filename}.csv",
        encoding='euc-kr'
    )
    npdat = dat.to_numpy()
    wd = [[_[0], _[4]] for _ in npdat]
    result = dict()
    for code, name in wd:
        result[__clean_value(name)] = code

    return result


if __name__ == '__main__':
    t1 = csv_to_dict('ifrs')
    t2 = csv_to_dict('gaap')
    t3 = csv_to_dict('consensus')
    t4 = csv_to_dict('stock')