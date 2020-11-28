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
                    break

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
                        break

            if result is not None:
                row[f'Multiple_listings_D_{type}'] = result

def Stock_indexing_D():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Stock_indexing_D_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = 0 if sr['STOCK INDEX INFORMATION'] in ['NA', 'NOT USED'] else 1
                    break

            if result is not None:
                row[f'Stock_indexing_D_{type}'] = result

def Multishare_Corporation_D():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Multishare_Corporation_D_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = 1 if sr['IND-CURRENTLY MULT-SHARE CO'] == 'X'  else 0
                    break

            if result is not None:
                row[f'Multishare_Corporation_D_{type}'] = result

def ADR_D():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'ADR_D_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = 1 if sr['INDICATOR - ADR'] == 'X'  else 0
                    break

            if result is not None:
                row[f'ADR_D_{type}'] = result

def Board_Structure_Type():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Board_Structure_Type_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = None if sr['Board Structure Type'] == 'NA' else sr['Board Structure Type']
                    break

            if result is not None:
                row[f'Board_Structure_Type_{type}'] = result

def Product_market():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Product_market_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = None if sr['PRODUCTS'] == 'NA' else str(sr['PRODUCTS']).lower()
                    break

            if result is not None:
                row[f'Product_market_{type}'] = result

def Level_2_sector_name():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Level_2_sector_name_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = sr['LEVEL2 SECTOR NAME']
                    break

            if result is not None:
                row[f'Level_2_sector_name_{type}'] = result

def Level_3_sector_name():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Level_3_sector_name_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = sr['LEVEL3 SECTOR NAME']
                    break

            if result is not None:
                row[f'Level_3_sector_name_{type}'] = result

def Level_4_sector_name():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Level_4_sector_name_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = sr['LEVEL4 SECTOR NAME']
                    break

            if result is not None:
                row[f'Level_4_sector_name_{type}'] = result

def Level_5_sector_name():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Level_5_sector_name_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = sr['LEVEL5 SECTOR NAME']
                    break

            if result is not None:
                row[f'Level_5_sector_name_{type}'] = result

def Local_index():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Local_index_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = sr['LOCAL INDEX']
                    break

            if result is not None:
                row[f'Local_index_{type}'] = result

def Datastream_local_index():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'Datastream_local_index_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = sr['DATASTREAM INDEX 3']
                    break

            if result is not None:
                row[f'Datastream_local_index_{type}'] = result

def ICB_industry_DS_index():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'ICB_industry_DS_index_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = sr['ICB Industry Index']
                    break
            if result is not None:
                row[f'ICB_industry_DS_index_{type}'] = result

def FT_sector_DS_index():
    for type, ticker_type in mapping.items():
        db.core_fields.append(f'FT_sector_DS_index_{type}')
        for row in db.core:
            result = None
            for sr in db.stock_meta_rows:
                if row[ticker_type] == sr['Type']:
                    result = sr['FT SECTOR Index']
                    break

            if result is not None:
                row[f'FT_sector_DS_index_{type}'] = result
