import http
from src import utils
import json
from datetime import datetime, timedelta
from src.countryinfo import countries
import pathlib
import os
import urllib


def reformat_value(val: str):
    return val \
        .replace('(', '') \
        .replace(')', '') \
        .replace('/', '_') \
        .replace('.', '_') \
        .replace(' ', '_') \
        .replace('-', '_') \
        .replace('&', '') \
        .replace('%', '') \
        .replace("'", '')


def reformat_dict(row: dict):
    pairs = []
    nrow = {}

    for k, v in row.items():
        new_key = reformat_value(k)
        pairs.append([new_key, v])
    for k, v in pairs:
        nrow[k] = v

    return nrow


def currentDir(_file_, path):
    return str(pathlib.PurePath(_file_).parent.joinpath(path))


def getCountryInfo(country):
    for c in countries:
        if c['name'] == country:
            return c


def parseDate(date: str):
    if date is None:
        return None

    if date.count('/') != 2:
        return None

    date_format = '%m/%d/%Y'
    return datetime.strptime(date, date_format)

def convert2pdfDate(date):
    date_format = '%-d %B %Y'
    return date.strftime(date_format)

def parsePdfDate(date: str):
    date_format = '%d %B %Y'
    return datetime.strptime(date, date_format)


def exists(property: str) -> bool:
    if property is None:
        return False
    return len(str(property).strip()) > 0


def absoluteFilePaths(directory):
    for dirpath, _, filenames in os.walk(directory):
        for f in filenames:
            yield os.path.abspath(os.path.join(dirpath, f))


def getCode(code):
    return str(code.split("(")[0]).strip()

def formatTicker(ticker):
    return str(ticker).strip()

def getName(name):
    names = [
        # New vars
        'CURRENT RATIO',
        'ACID TEST RATIO',
        'CASH RATIO',
        'DEBT RATIO',
        'DEBT TO EQUITY RATIO',
        'EQUITY RATIO',
        
        # Stare spremenljivke
        'NET SALES OR REVENUES',
        'COST OF GOODS SOLD/SALES',
        'TOTAL ASSETS',
        'CASH',
        'CURRENT ASSETS - TOTAL',
        'TOTAL INVENTORIES',
        'TOTAL LIABILITIES & SHAREHOLDE',
        'TOTAL LIABILITIES',
        "COMMON SHAREHOLDERS EQUITY",
        'LONG TERM DEBT',
        'SHORT TERM DEBT & CURRENT PORT',
        'RETAINED EARNINGS',
        'WORKING CAPITAL',
        'EARNINGS BEF INTEREST & TAXES',
        'EBIT & DEPRECIATION',
        'OPERATING INCOME',
        'OPERATING PROFIT MARGIN',
        'GROSS PROFIT MARGIN',
        'EARNINGS PER SHR',
        'EARNINGS PER SHARE',
        'EMPLOYEES',
        'ASSETS PER EMPLOYEE',
        'INVENTORY TURNOVER',
        'TOTAL ASSET TURNOVER',
        'Involuntary Turnover of Employees',
        'Voluntary Turnover of Employees',
        'EX-DIVID DATE',
        'DIV PAY DATE',
        'DIVIDEND TYPE',
        'DIVIDENDS PER SHARE - FISCAL',
        'DIVIDENDS PER SHARE',
        'DIVIDEND PAYOUT PER SHARE',
        'DIVIDEND YIELD',
        'RETURN ON ASSETS',
        'RETURN ON EQUITY - TOTAL (%)',
        'CURRENT LIABILITIES',
        'COST OF GOODS SOLD (EXCL DEP)',
        'NET INCOME - BASIC',
        'NET INCOME BEFORE PREFERRED DI',
    ]
    for n in names:
        if n in name:
            return n.\
                replace(' ', '_')\
                .replace('&_', '')\
                .replace('-_', '')\
                .replace('(', '')\
                .replace(')', '')\
                .replace("'", '')\
                .replace("%", '')\
                .lower().capitalize()

def get_annual_years(row):
    names_dates = []
    for k in row.keys():
        if k.isnumeric():
            names_dates.append(k)

    return names_dates

def create_annual_row(new_var, row):
    names_dates = get_annual_years(row)
    d = {
        'Name': row['Name'].split('-')[0] + f' - {new_var}',
        'Code': getCode(row['Code']),
        'CURRENCY': row['CURRENCY']
    }
    for da in names_dates:
        d[str(da)] = 0

    return d

def create_A1012M_MI_row(date_name: str, row: dict, core_row, VAR=None, TYPE=None, index=None, ticker=None):
    date = core_row[date_name]
    d = {
        'Type': TYPE,
        'Name': row['Name'],
        'Var': VAR,
        'currency_type': row.get('currency', None),
        'Ticker': ticker,
        'Index': index,
        'Code': row['Code'],
        'CURRENCY': row['CURRENCY'],
        'Date_type': date_name,
        'Date': f'{date.month}/{date.day}/{date.year}' if isinstance(date, datetime) else date
    }
    if isinstance(date, str):
        date = utils.parseDate(date)

    i = -300
    count = -300
    while True:
        ndate = date + timedelta(days=i)
        if ndate.weekday() not in [5, 6]:
            value = row.get(f'{ndate.month}/{ndate.day}/{ndate.year}', None)
            d[count] = value if value not in ['NA'] else None
            count += 1
        i+=1
        if count == 302:
            break
    return d

def find_closest_value(row, zero_day):
    for day_delta in range(0, 40):
        plus_day = row[zero_day + day_delta]
        minus_day = row[zero_day - day_delta]
        if plus_day is not None:
            return (float(plus_day), +day_delta)
        elif minus_day is not None:
            return (float(minus_day), -day_delta)

    return (None, None)
