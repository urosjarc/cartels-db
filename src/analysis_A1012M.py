from src import db, utils
dates_names = [
    'Infringement_begin',
    'Dawn_raid',
    'EC_Date_of_decision',
    'GC_Decision_date',
    "ECJ_Decision_date"
]
def adjusted_price():
    for row in db.core_A1012M['adjusted_price_local']:
        for core_row in db.core:
            if utils.getCode(row['Code']) == core_row['Ticker_firm']:
                for n in dates_names:
                    if core_row[n] is not None and core_row[n] != '':
                        new_row = utils.create_A1012M_row(n, row, core_row[n])
                        db.core_A1012M_all.append(new_row)