from src import db, utils, analysis_utils
from datetime import datetime
import sys

mapping = {
    'firm': 'Ticker_firm',
    'undertaking': 'Ticker_undertaking',
    'holding': 'Holding_Ticker_parent'
}

def Active_ticker_D():

    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Active_ticker_D_{type}')
        for row in db.core:

            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = 1 if sr['EQUITIES STATUS'] == 'ACT.' else 0

            if result is not None:
                row[f'Active_ticker_D_{type}'] = result

def Euro_currency_ticker_D():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Euro_currency_ticker_D_{type}')
        for row in db.core:

            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = 1 if sr['CURRENCY'] == 'E' else 0

            if result is not None:
                row[f'Euro_currency_ticker_D_{type}'] = result


def Active_date():
    new_vars = {
        "Dawn_raid": 'Dawn_raid',
        "EC_decision": 'EC_Date_of_decision',
        "GC_decision": 'GC_Decision_date',
        "ECJ_decision": 'ECJ_Decision_date'
    }

    for var, date_var in new_vars.items():
        for type, ticker_type in mapping.items():
            db.core_fields.append(f'{var}_active_ticker_{type}')
            for row in db.core:
                result = None
                for sr in db.stock_meta_rows:
                    if row[ticker_type] == sr['Type']:

                        date = utils.parseDate(row[date_var])
                        inactive_date = utils.parseDate(sr['DEAD DATE'])

                        if date is not None and inactive_date is not None:
                            result = 1 if date < inactive_date else 0

                if result is not None:
                    row[f'{var}_active_ticker_{type}'] = result

def Stock_exchange_name():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Stock_exchange_name_{type}')
        for row in db.core:

            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = sr['BOURSE NAME']

            if result is not None:
                row[f'Stock_exchange_name_{type}'] = result

def Multiple_listings_D():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Multiple_listings_D_{type}')
        for row in db.core:

            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    val = sr['STOCK EXCHANGE(S) LISTED']
                    if val != 'NA':
                        result = 1 if ',' in val else 0

            if result is not None:
                row[f'Multiple_listings_D_{type}'] = result
