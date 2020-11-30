from src import db, utils

def Ime_Currency_Firm_infringement_begin_year():
    colums = set()
    for i, rows_group in enumerate([db.A1012M_EU_rows, db.A1012M_LOCAL_rows]):
        currency = "euros" if i==0 else "local"
        for annual_ticker, annual_rows in rows_group.items():
            for core_row in db.core:
                if core_row['Ticker_firm'] == annual_ticker:
                    for annual_row in annual_rows:
                        value = annual_row.get(str(core_row['InfringeBeginYearFirm']), None)
                        name = utils.getName(annual_row['Name'])
                        if name is None:
                            raise Exception(annual_row['Name'])
                        colum_name = f'{name}__{currency}__InfringeBeginYearFirm'
                        core_row[colum_name] = value
                        colums.add(colum_name)

    count_local = 0
    count_eu= 0
    for col in sorted(list(colums)):
        print(col)
        if 'local' in col:
            count_local+= 1
        else:
            count_eu += 1
        db.core_fields.append(col)
    print(count_eu, count_local)



