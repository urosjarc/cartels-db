from datetime import timedelta
import pandas as pd

from src import db, utils
import re, os
import csv
import sys

this = sys.modules[__name__]

this.core = []
this.core_vars_changes = []
this.annual_figures_eu = []
this.annual_figures_local = []
this.annual_figures_eu_new = []
this.annual_figures_local_new = []
this.core_reformat = {}
this.A1012M_euro = []
this.A1012M_local = []
this.A1012M_euro_new = []
this.A1012M_local_new = []

this.ticker_returns_euro = []
this.ticker_returns_local = []


def init_core(with_new_vars=False):
    this.core = []
    with open(db.csvCorePathOut + ('.new_vars' if with_new_vars else ''), 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.core.append(row)
    this.core_vars = [key for key in this.core[0].keys()]


def init_core_reformat():
    p = f'{db.csvPath}/REFORMAT/core/'
    for f in os.listdir(p):
        name = f.split('.')[0]

        with open(p + f) as csvfile:
            vars = csvfile.read().split('\n')
            this.core_reformat[name] = []
            for l in vars:
                if len(l) > 0:
                    this.core_reformat[name].append(l)


def init_annual_figures():
    this.annual_figures_eu = []
    this.annual_figures_local = []

    with open(db.csvPath + '/annual_figures_eu.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.annual_figures_eu.append(row)

    with open(db.csvPath + '/annual_figures_local.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.annual_figures_local.append(row)


def init_A1012M():
    type = input("Vnesi tip A1012M [local/euro]: ")
    if type == 'local':
        this.A1012M_local = []
        with open(db.csvPath + '/A1012M_local.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                this.A1012M_local.append(row)
    else:
        this.A1012M_euro = []
        with open(db.csvPath + '/A1012M_euro.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                this.A1012M_euro.append(row)

    return type


def save_A1012M(type):
    if type == 'local':
        with open(db.csvPath + '/OUT_A1012M_local.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=this.A1012M_local_new[0].keys())
            writer.writeheader()
            writer.writerows(this.A1012M_local_new)
    else:
        with open(db.csvPath + '/OUT_A1012M_euro.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=this.A1012M_euro_new[0].keys())
            writer.writeheader()
            writer.writerows(this.A1012M_euro_new)


def save_ticker_returns(type):
    if type == 'local':
        with open(db.csvPath + '/OUT_ticker_returns_local.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=this.ticker_returns_local[0].keys())
            writer.writeheader()
            writer.writerows(this.ticker_returns_local)
    else:
        with open(db.csvPath + '/OUT_ticker_returns_euro.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=this.ticker_returns_euro[0].keys())
            writer.writeheader()
            writer.writerows(this.ticker_returns_euro)



def save_annual_figures():
    with open(db.csvPath + '/OUT_annual_figures_local.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=this.annual_figures_local_new[0].keys())
        writer.writeheader()
        writer.writerows(this.annual_figures_local_new)
    with open(db.csvPath + '/OUT_annual_figures_eu.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=this.annual_figures_eu_new[0].keys())
        writer.writeheader()
        writer.writerows(this.annual_figures_eu_new)


def save_long_vars():
    with open(f'{db.csvPath}/OUT_long_vars.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=['OLD', 'NEW'])
        writer.writeheader()
        writer.writerows(this.core_vars_changes)


def save_core():
    rows_groups = {}
    for name in this.core_reformat.keys():
        rows_groups[name] = []

    case_rows = []
    case_name = None
    for row in this.core:
        if case_name is None:
            case_name = row['Case']
        if case_name != row['Case']:
            new_row = {}
            for var in this.core_reformat['Case']:
                new_row[var] = row[var]
            case_rows.append(new_row)
            case_name = row['Case']

    u_rows = []
    u_name = None
    for row in this.core:
        if u_name is None:
            u_name = row['Case'] + row['Undertaking']
        if u_name != (row['Case'] + row['Undertaking']):
            new_row = {}
            for var in this.core_reformat['Undertaking']:
                new_row[var] = row[var]
            u_rows.append(new_row)
            u_name = row['Case'] + row['Undertaking']

    with open(f'{db.csvPath}/OUT_core_case.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=case_rows[0].keys())
        writer.writeheader()
        writer.writerows(case_rows)
    with open(f'{db.csvPath}/OUT_core_undertaking.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=u_rows[0].keys())
        writer.writeheader()
        writer.writerows(u_rows)


def change_long_vars():
    changes = {
        'ECJ_Total_confirmation_of_EC_decision_dissmisal_of_action_on_1st_instance': 'ECJ_TOT_CONF_EC_DEC',
        'undertaking': 'UD',
        'firm': 'F',
        'decision': 'DEC',
        'investigation': "INV",
        "infringe": "INF",
        'infringements': "INF",
        'percent': "PP",
        'additional': "ADD",
        'annulment': "ANN",
        'referral': 'REF',
        'duration': 'DUR',
        'year': 'Y',
        'commission': 'EC',
        'partial': 'PART',
        'local': 'L',
        'euros': 'E',
        'begin': 'BEG',
        'notice': 'N',
        'guidelines': 'G',
        'reduction': 'RED',
        'ratio': 'R',
        'trend': 'T',
        'holding': 'HD',
        'within': 'IN',
        'behavioral': 'BEH',
        'adjudicate': 'ADJ',
        'Change_of_other_remedies': 'COR',
        'inadmissible': 'INADM',
        'action': 'A',
        'dissmissing': 'DIS',
        'dismissing': 'DIS',
        'dissmissal': 'DIS',
        'dissmiss': 'DIS',
        '_of_': '_',
        '_for_': '_',
        'judgement': 'JUDGE',
        '___': '_',
        '__': '_',
        'Net_income_before_preferred_di_': 'NIPREF',
        'Current_assets_total_': 'CURASSET',
        'Cost_of_goods_sold/sales_': 'COGSS',
        'Earnings_bef_interest_taxes': 'EBIT',
        'Earnings_per_shr_': 'EPS',
        'Earnings_per_share': 'E_P_S',
        'Short_term_debt_current_port_': 'SHORT_DEBT',
        'Common_shareholders_equity_': 'EQUITY',
        'Long_term_debt_': 'LONG_DEBT',
        'Total_liabilities_shareholde_': 'TOTAL_PASSIVE',
        'Ebit_depreciation': 'EBIT_DEP',
        'Return_on_equity': 'ROE',
        'Dividend_payout_per_share_': 'DIV_PAY_SHARE',
        'Cost_of_goods_sold_excl_dep_': 'SOGS_EXCL_DEP',
        'Net_sales_or_revenues_': 'NET_SALES',
        'Operating_profit_margin_': 'OP_P_MARGIN',
        'Involuntary_turnover_of_employees_': 'INV_TUR_EMP',
        'Dividends_per_share_fiscal_': 'DIV_FIS',
        'Current_liabilities': 'CUR_LIAB',
        'Ex-divid_date': 'EXDIV_DATE',
        'Gross_profit_margin': 'GROSS_P_M',
        'Assets_per_employee': 'ASSETS_EMP',
        'Total_inventories': 'TOT_INV',
        'Operating_income': 'OP_INCOME',
        'Return_on_assets': 'ROA',
        'Total_liabilities': 'TOT_LIAB',
        'Retained_earnings': 'RETA_EARNING',
        'Involuntary_turnover_employees': 'INV_TURN_EMP',
        'Debt_to_equity': 'DEBT_EQUI',
        'Cost_goods_sold_excl_dep': 'COGS_EX_DEP',
        'Inventory_turnover': 'INV_TURN',
        'Net_income_basic': 'NET_INC_BASIC',
        'Total_asset_turnover': 'TOT_ASSET_TURN',
        'Dividends_per_share': 'DIV_P_SHARE',
        'Cost_goods_sold/sales': 'COGSS',
        'Voluntary_turnover_employees': 'VOL_TURN_EMP',
        'Dawn_raid': 'DR',
        'Total_assets': 'TOT_ASSETS',
        'Dividend_type': 'DIV_TYPE',
        'Working_capital': 'WORK_CAP',
        'Dividend_yield': 'DIV_YIELD',
        '_HD_HD': '_HD',
        '_Fine_': '_FIN_',
        'change': 'CHG',
        'only': 'ONY',
        'case': "C",
        'Total_party_appeal_success': 'TOT_PARTY_APP_SUCC',
        'Pecularities': 'PECS',

    }
    core_changes = {}
    for key in this.core[0].keys():
        if len(key) > 32:
            newkey = key
            for old, new in changes.items():
                pattern = re.compile(old, re.IGNORECASE)
                newkey = pattern.sub(new, newkey)
            core_changes[key] = newkey
            this.core_vars_changes.append({
                'OLD': key,
                'NEW': newkey
            })

    for name, vars in this.core_reformat.items():
        new_vars = []
        for var in vars:
            newkey = var
            if len(newkey) > 32:
                for old, new in changes.items():
                    pattern = re.compile(old, re.IGNORECASE)
                    newkey = pattern.sub(new, newkey)
            newkey = newkey \
                .replace('undertaking', 'UD') \
                .replace('trend', 'T') \
                .replace('__', '_') \
                .replace('case', 'C') \
                .replace('only', 'ONY')
            new_vars.append(newkey)
        this.core_reformat[name] = new_vars

    core_lines = open(db.csvCorePathOut, 'r').readlines()
    for old, new in core_changes.items():
        core_lines[0] = core_lines[0].replace(old, new)

    core_lines[0] = core_lines[0] \
        .replace('undertaking', 'UD') \
        .replace('trend', 'T') \
        .replace('__', '_') \
        .replace('case', 'C') \
        .replace('only', 'ONY')
    with open(db.csvCorePathOut + '.new_vars', 'w') as file:
        file.writelines(core_lines)


def change_annual_structure():
    mapping = {
        '%EARNINGS PER SHR': 'EARNINGS PER SHR',
        'ACID TEST RATIO': 'ACID TEST RATIO',
        'ASSETS PER EMPLOYEE': 'ASSETS PER EMPLOYEE',
        'BASIC': 'NET INCOME BASIC',
        'CASH': 'CASH',
        'CASH RATIO': 'CASH RATIO',
        'COMMON SHAREHOLDERS EQUITY': 'COMMON SHAREHOLDERS EQUITY',
        'COST OF GOODS SOLD (EXCL DEP)': 'COST OF GOODS SOLD (EXCL DEP)',
        'CURRENT LIABILITIES-TOTAL': 'CURRENT LIABILITIES-TOTAL',
        'CURRENT RATIO': 'CURRENT RATIO',
        'DEBT RATIO': 'DEBT RATIO',
        'DEBT TO EQUITY RATIO': 'DEBT TO EQUITY RATIO',
        'DIVIDENDS PER SHARE': 'DIVIDENDS PER SHARE',
        'EARNINGS BEF INTEREST & TAXES': 'EARNINGS BEF INTEREST & TAXES',
        'EARNINGS PER SHARE': 'EARNINGS PER SHARE',
        'EARNINGS PER SHR': 'EARNINGS PER SHR',
        'EBIT & DEPRECIATION': 'EBIT & DEPRECIATION',
        'EQUITY RATIO': 'EQUITY RATIO',
        'FISCAL': 'DIVIDENDS PER SHARE FISCAL',
        'LONG TERM DEBT': 'LONG TERM DEBT',
        'NET INCOME BEFORE PREFERRED DI': 'NET INCOME BEFORE PREFERRED DI',
        'NET OPERATING INCOME': 'NET OPERATING INCOME',
        'NET SALES OR REVENUES': 'NET SALES OR REVENUES',
        'OPERATING INCOME': 'OPERATING INCOME',
        'RETAINED EARNINGS': 'RETAINED EARNINGS',
        'SHORT TERM DEBT & CURRENT PORT': 'SHORT TERM DEBT & CURRENT PORT',
        'TOTAL': 'CURRENT ASSETS TOTAL',
        'TOTAL ASSETS': 'TOTAL ASSETS',
        'TOTAL INVENTORIES': 'TOTAL INVENTORIES',
        'TOTAL LIABILITIES': 'TOTAL LIABILITIES',
        'TOTAL LIABILITIES & SHAREHOLDE': 'TOTAL LIABILITIES & SHAREHOLDE',
        'WORKING CAPITA': 'WORKING CAPITAL',
        'WORKING CAPITAL': 'WORKING CAPITAL',
        'COST OF GOODS SOLD/SALES': 'COST OF GOODS SOLD SALES',
        'OPERATING PROFIT MARGIN': 'OPERATING PROFIT MARGIN',
        'GROSS PROFIT MARGIN': 'GROSS PROFIT MARGIN',
        'EMPLOYEES': 'EMPLOYEES',
        'INVENTORY TURNOVER': 'INVENTORY TURNOVER',
        'TOTAL ASSET TURNOVER': 'TOTAL ASSET TURNOVER',
        '%EX-DIVID DATE': 'EX DIVID DATE',
        'EX-DIVID DATE': 'EX DIVID DATE',
        'DIV PAY DATE': 'DIV PAY DATE',
        '%DIV PAY DATE': 'DIV PAY DATE',
        'DIVIDEND TYPE': 'DIVIDEND TYPE',
        '%DIVIDEND TYPE': 'DIVIDEND TYPE',
        'DIVIDEND PAYOUT PER SHARE': 'DIVIDEND PAYOUT PER SHARE',
        'DIVIDEND YIELD': 'DIVIDEND YIELD',
        '%DIVIDEND YIELD': 'DIVIDEND YIELD',
        'RETURN ON ASSETS': 'RETURN ON ASSETS',
        'TOTAL (%)': 'RETURN ON EQUITY PERCENT',
        'Voluntary Turnover of Employees': 'VOLUNTARY TURNOVER OF EMPLOY',
        'Involuntary Turnover of Employees': 'INVOLUNTARY TURNOVER OF EMPLOY'
    }
    unique_mapping = set()
    for row in this.annual_figures_eu + this.annual_figures_local:
        name = row['Name'].split(' - ')[-1]
        mapping[name] = mapping[name] \
            .replace('&', '') \
            .replace('-', ' ') \
            .replace('  ', ' ') \
            .replace('(', '') \
            .replace(')', '')
        unique_mapping.add(mapping[name])

    unique_tickers = set()
    years = set()
    for row in this.annual_figures_local + this.annual_figures_eu:
        ticker = utils.getCode(row['Code'])
        if len(ticker) > 2:
            unique_tickers.add(ticker)
        for key in row.keys():
            if str(key).isnumeric():
                years.add(key)
    years = sorted(years)

    new_format_local = {}
    new_format_eu = {}
    tn = 0
    for ticker in unique_tickers:
        print('Ticker:', tn)
        tn += 1
        new_format_local[ticker] = {}
        new_format_eu[ticker] = {}
        for year in years:
            type_dict = {}
            for row in this.annual_figures_local:
                row_ticker = utils.getCode(row['Code'])
                if row_ticker == ticker:
                    row_type = mapping[row['Name'].split(' - ')[-1]]
                    year_value = row[year]
                    type_dict[row_type] = year_value
            new_format_local[ticker][year] = type_dict

            type_dict = {}
            for row in this.annual_figures_eu:
                row_ticker = utils.getCode(row['Code'])
                if row_ticker == ticker:
                    row_type = mapping[row['Name'].split(' - ')[-1]]
                    year_value = row[year]
                    type_dict[row_type] = year_value
            new_format_eu[ticker][year] = type_dict

    for ticker, ticker_dict in new_format_local.items():
        for year, year_dict in ticker_dict.items():
            new_row = {
                'ticker': ticker,
                'year': year
            }
            for type, val in year_dict.items():
                try:
                    float(val)
                    new_row[type] = val
                except:
                    new_row[type] = None
            for u_type in unique_mapping:
                if u_type not in new_row:
                    new_row[u_type] = None
            this.annual_figures_local_new.append(new_row)
    for ticker, ticker_dict in new_format_eu.items():
        for year, year_dict in ticker_dict.items():
            new_row = {
                'ticker': ticker,
                'year': year
            }
            for type, val in year_dict.items():
                try:
                    float(val)
                    new_row[type] = val
                except:
                    new_row[type] = None
            for u_type in unique_mapping:
                if u_type not in new_row:
                    new_row[u_type] = None
            this.annual_figures_eu_new.append(new_row)


def change_A1012M_structure(type):
    rel2 = {}
    rel4 = {}
    for row in db.REL_STOCK_LEV2IN:
        rel2[row['name']] = row['code']
    for row in db.REL_STOCK_LEV4SE:
        rel4[row['name']] = row['code']
    row_group = this.A1012M_local if type == 'local' else this.A1012M_euro
    for row in row_group:
        ticker = utils.getCode(row['Code'])
        Market_DSLOC = None
        Market_MLOC = None
        Market_LEV2IN = None
        Market_LEV4SE = None
        for static_row in db.stock_meta_rows:
            if ticker == utils.formatTicker(static_row['Type']):
                Market_DSLOC = static_row['DATASTREAM INDEX']
                Market_MLOC = static_row['LOCAL INDEX']
                Market_LEV2IN = rel2[static_row['LEVEL2 SECTOR NAME']]
                Market_LEV4SE = rel4[static_row['LEVEL4 SECTOR NAME'].replace(",", "")]
                break

        new_row = {
            'Date': row['Date'],
            'Competition_event': row['Date_type'],
            'Ticker': ticker,
            'Market_DSLOC': Market_DSLOC,
            'Market_MLOC': Market_MLOC,
            'Market_LEV2IN': Market_LEV2IN,
            'Market_LEV4SE': Market_LEV4SE,
            'Market_TOTMKWD': 'TOTMKWD'
        }
        if type == 'local':
            this.A1012M_local_new.append(new_row)
        else:
            this.A1012M_euro_new.append(new_row)


def create_ticker_returns(type):
    rel2 = {}
    rel4 = {}
    for row in db.REL_STOCK_LEV2IN:
        rel2[row['name']] = row['code']
    for row in db.REL_STOCK_LEV4SE:
        rel4[row['name']] = row['code']

    tickers_dict = {}
    for static_row in db.stock_meta_rows:
        tickers_dict[utils.getCode(static_row['Type'])] = {}



    c=0
    for chunk in pd.read_csv(db.csvPath + f'/A1012M_{type}.csv', chunksize=514):
        c+=1
        if c==50:
            break
        print('Complete:', c)
        for i, row in chunk.iterrows():
            ticker = utils.getCode(row['Code'])
            tickers_dict[ticker][row['Var']] = []
            Market_DSLOC = None
            Market_MLOC = None
            Market_LEV2IN = None
            Market_LEV4SE = None
            for static_row in db.stock_meta_rows:
                if ticker == utils.formatTicker(static_row['Type']):
                    Market_DSLOC = static_row['DATASTREAM INDEX']
                    Market_MLOC = static_row['LOCAL INDEX']
                    Market_LEV2IN = rel2[static_row['LEVEL2 SECTOR NAME']]
                    Market_LEV4SE = rel4[static_row['LEVEL4 SECTOR NAME'].replace(",", "")]
                    break

            date0 = utils.parseDate(row['Date'])
            start_number = -1 #-300
            finish_number = 2 #101
            days = start_number
            count = start_number

            while(True):
                nextDate = date0 + timedelta(days = days)
                if nextDate.weekday() not in [5, 6]:
                    num = row[str(count)]
                    new_row = {
                        'Date': f'{nextDate.month}/{nextDate.day}/{nextDate.year}',
                        row['Var']: num if str(num)!='nan' else None,
                        'Ticker': ticker,
                        'Market_DSLOC': Market_DSLOC,
                        'Market_MLOC': Market_MLOC,
                        'Market_LEV2IN': Market_LEV2IN,
                        'Market_LEV4SE': Market_LEV4SE,
                        'Market_TOTMKWD': 'TOTMKWD'
                    }
                    tickers_dict[ticker][row['Var']].append(new_row)
                    count += 1
                    if count == finish_number:
                        break
                days += 1

    new_rows = []
    for ticker, tickers_d in tickers_dict.items():
        if len(tickers_d.keys()) == 0:
            continue
        for i in range(401):
            first_row = None
            for var, row_list in tickers_d.items():
                try:
                    row = row_list[i]
                    if first_row is None:
                        first_row = row
                    for k, v in row.items():
                        if k not in [ 'Date', 'Ticker', 'Market_DSLOC', 'Market_MLOC', 'Market_LEV2IN', 'Market_LEV4SE', 'Market_TOTMKWD' ]:
                            first_row[k] = v if str(v)!='nan' else None
                except Exception:
                    pass

            if first_row is not None and isinstance(first_row, dict):
                new_rows.append(first_row)

    if type == 'local':
        this.ticker_returns_local = new_rows
    else:
        this.ticker_returns_euro = new_rows




    pass

if __name__ == '__main__':
    # RESTRUCTURE CORE==========
    # init_core(with_new_vars=False)
    # init_core_reformat()
    # change_long_vars()
    # init_core(with_new_vars=True)
    # save_long_vars()
    # save_core()
    # RESTRUCTURE CORE

    # RESTRUCTURE ANNUAL=========
    # init_annual_figures()
    # change_annual_structure()
    # save_annual_figures()
    # RESTRUCTURE ANNUAL=========

    # CREATE INDEX FILE==========
    # db.init_nodes_stock_meta()
    db.init_rel_stock()
    # type = init_A1012M()
    # change_A1012M_structure(type)
    # save_A1012M(type)
    # CREATE INDEX FILE==========

    # CREATE ticker returns file =======
    db.init_nodes_stock_meta()
    type = input('local/euro?: ')
    create_ticker_returns(type)
    save_ticker_returns(type)

    # CREATE ticker returns file =======
