from src import db, utils
from math import log, e
from copy import deepcopy
dates_names = [
    'Infringement_begin',
    'Investigation_begin_without_dawn_raid',
    'Dawn_raid',
    'EC_Date_of_decision',
    'GC_Decision_date',
    "ECJ_Decision_date",
]
def NAMES_A1012(name: str, type):
    if type == 'local':
        for row in db.core_A1012M_local[name]:
            if row['Name'] == '#ERROR':
                continue
            for core_row in db.core:
                ticker_code = utils.getCode(row['Code'])
                if ticker_code is not None and ticker_code in [core_row['Ticker_firm'], core_row['Ticker_undertaking'], core_row['Holding_Ticker_parent']]:
                    for n in dates_names:
                        if core_row[n] is not None and core_row[n] != '':
                            new_row = utils.create_A1012M_row(n, row, core_row[n], VAR=name)
                            db.core_A1012M_all_local.append(new_row)

    if type == 'euro':
        for row in db.core_A1012M_euro[name]:
            if row['Name'] == '#ERROR':
                continue
            for core_row in db.core:
                ticker_code = utils.getCode(row['Code'])
                if ticker_code is not None and ticker_code in [core_row['Ticker_firm'], core_row['Ticker_undertaking'], core_row['Holding_Ticker_parent']]:
                    for n in dates_names:
                        if core_row[n] is not None and core_row[n] != '':
                            new_row = utils.create_A1012M_row(n, row, core_row[n], VAR=name)
                            db.core_A1012M_all_euro.append(new_row)

def momentum_year(type):
    print('momentum_year', type)
    dict_type = db.core_A1012M_all_local if type == 'local' else db.core_A1012M_all_euro
    deltas = []
    for row in dict_type:
        day0, day0_delta = utils.find_closest_value(row, 0)
        day260, day260_delta = utils.find_closest_value(row, -260)
        if day0_delta is not None:
            deltas.append(day0_delta)
        if day260_delta is not None:
            deltas.append(day260_delta)

        if None not in [day0, day260]:
            if day260 > 0:
                row['Momentum_year'] = day0/day260
                row['Momentum_year_delta0'] = day0_delta
                row['Momentum_year_delta260'] = day260_delta
            else:
                row['Momentum_year'] = None
        else:
            row['Momentum_year'] = None

def ln_returns(type):
    print('ln_returns', type)
    dict_type = db.core_A1012M_all_local if type == 'local' else db.core_A1012M_all_euro
    for row in dict_type:
        if row['Var'] in ['unadjusted_price', 'adjusted_price', 'turnover_volume', 'price_index']:
            row_copy = deepcopy(row)
            for i in range(-300, 301):
                today = row[i+1]
                yesterday = row[i]
                if None not in [today, yesterday]:
                    today = float(today)
                    yesterday = float(yesterday)
                    if yesterday > 0 and today > 0:
                        row_copy[i] = log(today, e)- log(yesterday, e)
                    else:
                        row_copy[i] = None
                else:
                    row_copy[i] = None
            dict_type.append(row_copy)

def raw_returns(type):
    print('raw_returns', type)
    dict_type = db.core_A1012M_all_local if type == 'local' else db.core_A1012M_all_euro
    for row in dict_type:
        if row['Var'] in ['unadjusted_price', 'adjusted_price', 'turnover_volume', 'price_index']:
            row_copy = deepcopy(row)
            for i in range(-300, 301):
                today = row[i+1]
                yesterday = row[i]
                if None not in [today, yesterday]:
                    today = float(today)
                    yesterday = float(yesterday)
                    if yesterday > 0:
                        row_copy[i] = (today-yesterday)/yesterday
                    else:
                        row_copy[i] = None
                else:
                    row_copy[i] = None
            dict_type.append(row_copy)
