from src import db, utils

def Ime_Currency_VAR(VAR, ticker='Ticker_firm', year_minus=0):
    print("ANALYSIS CORE ANNUAL: ", VAR, year_minus)
    colums = set()
    for i, rows_group in enumerate([db.A1012M_EU_rows, db.A1012M_LOCAL_rows]):
        currency = "euros" if i==0 else "local"
        for annual_ticker, annual_rows in rows_group.items():
            for core_row in db.core:
                if core_row[ticker] == annual_ticker:
                    for annual_row in annual_rows:
                        if core_row[VAR] in [None, '']:
                            continue
                        value = annual_row.get(str(int(core_row[VAR])-year_minus), None)
                        name = utils.getName(annual_row['Name'])
                        if name is None:
                            raise Exception(annual_row['Name'])
                        colum_name = f'{name}__{currency}__{VAR}' + '' if year_minus == 0 else f'-{year_minus}'
                        core_row[colum_name] = value
                        colums.add(colum_name)

    for c in colums:
        db.core_fields.append(c)
