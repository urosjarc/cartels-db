from src import db, utils

def Ime_Currency_VAR_Info(VAR, ticker='Ticker_firm', year_minus=0, info=''):
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

                        name = utils.getName(annual_row['Name'])
                        if name in ['EX-DIVID DATE', 'DIV PAY DATE', 'DIVIDEND TYPE']:
                            continue
                        if name is None:
                            raise Exception(annual_row['Name'])

                        value = annual_row.get(str(int(core_row[VAR])-year_minus), None)
                        colum_name = f'{name}__{currency}__{VAR}_{info}' + '' if year_minus == 0 else f'-{year_minus}'
                        core_row[colum_name] = value
                        colums.add(colum_name)

    for c in colums:
        db.core_fields.append(c)

def Ime_Currency_VAR_Info_YearTrend(VAR, trend_year, ticker='Ticker_firm', info=''):
    print("ANALYSIS CORE ANNUAL TREND: ", VAR, trend_year)
    colums = set()
    for i, rows_group in enumerate([db.A1012M_EU_rows, db.A1012M_LOCAL_rows]):
        currency = "euros" if i==0 else "local"
        for annual_ticker, annual_rows in rows_group.items():
            for core_row in db.core:
                if core_row[ticker] == annual_ticker:
                    for annual_row in annual_rows:
                        if core_row[VAR] in [None, '']:
                            continue
                        name = utils.getName(annual_row['Name'])
                        if name in ['EX-DIVID DATE', 'DIV PAY DATE', 'DIVIDEND TYPE']:
                            continue
                        if name is None:
                            raise Exception(annual_row['Name'])

                        value0 = annual_row.get(str(int(core_row[VAR])), None)
                        valueX = annual_row.get(str(int(core_row[VAR])-trend_year), None)
                        colum_name = f'{name}__{currency}__{VAR}_{info}_{trend_year}trend'
                        try:
                            if None not in [valueX, value0] and 'NA' not in [value0, valueX] and float(valueX) != 0:
                                core_row[colum_name] = float(value0)/float(valueX)
                        except Exception:
                            pass
                        colums.add(colum_name)

    for c in colums:
        db.core_fields.append(c)
