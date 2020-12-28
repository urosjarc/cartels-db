from src import db, utils
from src.utils import formatTicker
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


def NAMES_DSLOC():
    for VAR, rows in db.core_market_indices['DSLOC'].items():
        print('DSLOC', VAR)
        for row in rows:
            code = utils.getCode(row['Code'])
            for meta_row in db.stock_meta_rows:
                if meta_row['DATASTREAM INDEX'] == code:
                    ticker_code = formatTicker(meta_row['Type'])
                    for core_row in db.core:
                        if ticker_code in [formatTicker(core_row['Ticker_firm']), formatTicker(core_row['Ticker_undertaking']),
                                           formatTicker(core_row['Holding_Ticker_parent'])]:
                            for n in dates_names:
                                if core_row[n] is not None and core_row[n] != '':
                                    new_row = utils.create_A1012M_MI_row(n, row, core_row, VAR=VAR, TYPE='DSLOC',
                                                                         ticker=ticker_code, index=code)
                                    db.core_market_indices_all.append(new_row)


def NAMES_TOTMKWD():
    for VAR, rows in db.core_market_indices['TOTMKWD'].items():
        print('TOTMKWD', VAR)
        row = rows[0]
        code = utils.getCode(row['Code'])
        for meta_row in db.stock_meta_rows:
            ticker_code = formatTicker(meta_row['Type'])
            for core_row in db.core:
                if ticker_code in [formatTicker(core_row['Ticker_firm']), formatTicker(core_row['Ticker_undertaking']),
                                   formatTicker(core_row['Holding_Ticker_parent'])]:
                    for n in dates_names:
                        if core_row[n] is not None and core_row[n] != '':
                            new_row = utils.create_A1012M_MI_row(n, row, core_row, VAR=VAR, TYPE='TOTMKWD',
                                                                 ticker=ticker_code, index=code)
                            db.core_market_indices_all.append(new_row)


def NAMES_MLOC():
    for VAR, rows in db.core_market_indices['MLOC'].items():
        print('MLOC', VAR)
        for row in rows:
            code = utils.getCode(row['Code'])
            for meta_row in db.stock_meta_rows:
                if meta_row['LOCAL INDEX'] == code:
                    ticker_code = formatTicker(meta_row['Type'])
                    for core_row in db.core:
                        if ticker_code in [formatTicker(core_row['Ticker_firm']), formatTicker(core_row['Ticker_undertaking']),
                                           formatTicker(core_row['Holding_Ticker_parent'])]:
                            for n in dates_names:
                                if core_row[n] is not None and core_row[n] != '':
                                    new_row = utils.create_A1012M_MI_row(n, row, core_row, VAR=VAR, TYPE='MLOC',
                                                                         ticker=ticker_code, index=code)
                                    db.core_market_indices_all.append(new_row)


def NAMES_LEV2IN():
    for VAR, rows in db.core_market_indices['2IN'].items():
        print('LEV2IN', VAR)
        for row in rows:
            code = utils.getCode(row['Code'])
            industry_name = None
            for rel_row in db.REL_STOCK_LEV2IN:
                if rel_row['code'] == code:
                    industry_name = rel_row['name']
            for meta_row in db.stock_meta_rows:
                if meta_row['LEVEL2 SECTOR NAME'] == industry_name:
                    ticker_code = formatTicker(meta_row['Type'])
                    for core_row in db.core:
                        if ticker_code in [formatTicker(core_row['Ticker_firm']), formatTicker(core_row['Ticker_undertaking']),
                                           formatTicker(core_row['Holding_Ticker_parent'])]:
                            for n in dates_names:
                                if core_row[n] is not None and core_row[n] != '':
                                    new_row = utils.create_A1012M_MI_row(n, row, core_row, VAR=VAR, TYPE='LEV2IN',
                                                                         ticker=ticker_code, index=code)
                                    db.core_market_indices_all.append(new_row)


def NAMES_LEV4SE():
    for VAR, rows in db.core_market_indices['4SE'].items():
        print('LEV4SE', VAR)
        for row in rows:
            code = utils.getCode(row['Code'])
            industry_name = None
            for rel_row in db.REL_STOCK_LEV4SE:
                if rel_row['code'] == code:
                    industry_name = rel_row['name']
                    break
            for meta_row in db.stock_meta_rows:
                if meta_row['LEVEL4 SECTOR NAME'] == industry_name:
                    ticker_code = formatTicker(meta_row['Type'])
                    for core_row in db.core:
                        if ticker_code in [formatTicker(core_row['Ticker_firm']), formatTicker(core_row['Ticker_undertaking']),
                                           formatTicker(core_row['Holding_Ticker_parent'])]:
                            for n in dates_names:
                                if core_row[n] is not None and core_row[n] != '':
                                    new_row = utils.create_A1012M_MI_row(n, row, core_row, VAR=VAR, TYPE='LEV4SE',
                                                                         ticker=ticker_code, index=code)
                                    db.core_market_indices_all.append(new_row)


def momentum_year():
    dict_type = db.core_market_indices_all
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
                row['Momentum_year'] = day0 / day260
                row['Momentum_year_delta0'] = day0_delta
                row['Momentum_year_delta260'] = day260_delta
            else:
                row['Momentum_year'] = None
                row['Momentum_year_delta0'] = None
                row['Momentum_year_delta260'] = None
        else:
            row['Momentum_year'] = None
            row['Momentum_year_delta0'] = None
            row['Momentum_year_delta260'] = None


def ln_returns():
    dict_type = db.core_market_indices_all
    rows = []
    for row in dict_type:
        row_copy = deepcopy(row)
        if 'ln_returns' not in row_copy['Var'] and 'raw_returns' not in row_copy['Var']:
            row_copy['Var'] += 'ln_returns'
            for i in range(-300, 301):
                today = row[i + 1]
                yesterday = row[i]
                if None not in [today, yesterday]:
                    today = float(today)
                    yesterday = float(yesterday)
                    if yesterday > 0 and today > 0:
                        row_copy[i] = log(today, e) - log(yesterday, e)
                    else:
                        row_copy[i] = None
                else:
                    row_copy[i] = None
            rows.append(row_copy)
    dict_type += rows


def raw_returns():
    dict_type = db.core_market_indices_all
    rows = []
    for row in dict_type:
        row_copy = deepcopy(row)
        if 'ln_returns' not in row_copy['Var'] and 'raw_returns' not in row_copy['Var']:
            row_copy['Var'] += '_raw_returns'
            for i in range(-300, 301):
                today = row[i + 1]
                yesterday = row[i]
                if None not in [today, yesterday]:
                    today = float(today)
                    yesterday = float(yesterday)
                    if yesterday > 0:
                        row_copy[i] = (today - yesterday) / yesterday
                    else:
                        row_copy[i] = None
                else:
                    row_copy[i] = None
            rows.append(row_copy)
    dict_type += rows
