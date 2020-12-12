from src import db, utils
dates_names = [
    'Infringement_begin',
    'Dawn_raid',
    'EC_Date_of_decision',
    'GC_Decision_date',
    "ECJ_Decision_date"
]
def NAMES_A1012(name: str, type, ticker: str='Ticker_firm'):
    if type == 'local':
        for row in db.core_A1012M_local[name]:
            for core_row in db.core:
                if utils.getCode(row['Code']) == core_row[ticker]:
                    for n in dates_names:
                        if core_row[n] is not None and core_row[n] != '':
                            new_row = utils.create_A1012M_row(n, row, core_row[n])
                            new_row['Var'] = name
                            db.core_A1012M_all_local.append(new_row)

    if type == 'euro':
        for row in db.core_A1012M_euro[name]:
            for core_row in db.core:
                if utils.getCode(row['Code']) == core_row[ticker]:
                    for n in dates_names:
                        if core_row[n] is not None and core_row[n] != '':
                            new_row = utils.create_A1012M_row(n, row, core_row[n])
                            new_row['Var'] = name
                            db.core_A1012M_all_euro.append(new_row)
