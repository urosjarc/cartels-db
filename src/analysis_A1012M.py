from src import db, utils
dates_names = [
    'Infringement_begin',
    'EC_Date_of_decision',
    'Dawn_raid'
]
def adjusted_price():
    for row in db.core_A1012M:
        for core_row in db.core:
            if utils.getCode(row['Code']) == core_row['Ticker_firm']:
                for n in dates_names:
                    new_row = utils.create_A1012M_row(n, row, core_row[n])
                    db.core_A1012M_all.append(new_row)