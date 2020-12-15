from src import db, utils
import sys

this = sys.modules[__name__]
this.COLUMS_Ime_Currency_VAR_Info = []
this.COLUMS_Ime_Currency_VAR_Info_YearTrend = []

matcher = {
    'Ticker_firm': 'firm',
    'Ticker_undertaking': 'undertaking',
    'Holding_Ticker_parent': 'holding'
}

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
                        colum_name = f'{name}__{currency}__{VAR}__{matcher[ticker]}__{info}' + ('' if year_minus == 0 else f'{year_minus}')
                        core_row[colum_name] = value
                        colums.add(colum_name)

    for c in colums:
        db.core_fields.append(c)

    this.COLUMS_Ime_Currency_VAR_Info += colums

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
                        colum_name = f'{name}__{currency}__{VAR}__{matcher[ticker]}__{info}_{trend_year}trend'
                        try:
                            if None not in [valueX, value0] and 'NA' not in [value0, valueX] and float(valueX) != 0:
                                core_row[colum_name] = float(value0)/float(valueX)
                        except Exception:
                            pass
                        colums.add(colum_name)

    for c in colums:
        db.core_fields.append(c)

    this.COLUMS_Ime_Currency_VAR_Info_YearTrend += colums

def Ime__Currency__Infr_begin_to_Inv_Beg_trend():
    colums = set()
    for e1 in this.COLUMS_Ime_Currency_VAR_Info:
        s1 = e1.split('__')
        for e2 in this.COLUMS_Ime_Currency_VAR_Info:
            s2 = e2.split('__')
            if s1[0] == s2[0] and s1[1] == s2[1] and s1[-1].startswith('Investigation_begin_year') and s2[-1].startswith('InfringeBeginYearFirm'):
                if s1[0] in ['Dividend_type', 'Div_pay', 'Div_pay_date']:
                    continue
                colum_name = f'{s1[0]}__{s1[1]}__Infr_begin_to_Inv_Beg_trend'
                colums.add(colum_name)
                for row in db.core:
                    v1 = row.get(e1, None)
                    v2 = row.get(e2, None)
                    if None not in [v2, v1] and 'NA' not in [v2, v1]:
                        if float(v1) != 0:
                            row[colum_name] = float(v2)/float(v1)
    for c in colums:
        db.core_fields.append(c)

def Ime__Currency__Infr_begin_to_EC_Dec_trend():
    colums = set()
    for e1 in this.COLUMS_Ime_Currency_VAR_Info:
        s1 = e1.split('__')
        for e2 in this.COLUMS_Ime_Currency_VAR_Info:
            s2 = e2.split('__')
            if s1[0] == s2[0] and s1[1] == s2[1] and s1[-1].startswith('EC_decision_year') and s2[-1].startswith('InfringeBeginYearFirm'):
                if s1[0] in ['Dividend_type', 'Div_pay', 'Div_pay_date']:
                    continue
                colum_name = f'{s1[0]}__{s1[1]}__Infr_begin_to_EC_Dec_trend'
                colums.add(colum_name)
                for row in db.core:
                    v1 = row.get(e1, None)
                    v2 = row.get(e2, None)
                    if None not in [v2, v1] and 'NA' not in [v2, v1]:
                        if float(v1) != 0:
                            row[colum_name] = float(v2)/float(v1)
    for c in colums:
        db.core_fields.append(c)

def Ime__Currency__Inv_begin_to_EC_Dec_trend():
    colums = set()
    for e1 in this.COLUMS_Ime_Currency_VAR_Info:
        s1 = e1.split('__')
        for e2 in this.COLUMS_Ime_Currency_VAR_Info:
            s2 = e2.split('__')
            if s1[0] == s2[0] and s1[1] == s2[1] and s1[-1].startswith('EC_decision_year') and s2[-1].startswith('Investigation_begin_year'):
                if s1[0] in ['Dividend_type', 'Div_pay', 'Div_pay_date']:
                    continue
                colum_name = f'{s1[0]}__{s1[1]}__Inv_begin_to_EC_Dec_trend'
                colums.add(colum_name)
                for row in db.core:
                    v1 = row.get(e1, None)
                    v2 = row.get(e2, None)
                    if None not in [v2, v1] and 'NA' not in [v2, v1]:
                        if float(v1) != 0:
                            row[colum_name] = float(v2)/float(v1)
    for c in colums:
        db.core_fields.append(c)

def Ime__Currency__EC_Dec_to_GC_Dec_trend():
    colums = set()
    for e1 in this.COLUMS_Ime_Currency_VAR_Info:
        s1 = e1.split('__')
        for e2 in this.COLUMS_Ime_Currency_VAR_Info:
            s2 = e2.split('__')
            if s1[0] == s2[0] and s1[1] == s2[1] and s1[-1].startswith('EC_decision_year') and s2[-1].startswith('GC_decision_year'):
                if s1[0] in ['Dividend_type', 'Div_pay', 'Div_pay_date']:
                    continue
                colum_name = f'{s1[0]}__{s1[1]}__EC_Dec_to_GC_Dec_trend'
                colums.add(colum_name)
                for row in db.core:
                    v1 = row.get(e1, None)
                    v2 = row.get(e2, None)
                    if None not in [v2, v1] and 'NA' not in [v2, v1]:
                        if float(v2) != 0:
                            row[colum_name] = float(v1)/float(v2)
    for c in colums:
        db.core_fields.append(c)

def Ime__Currency__GC_Dec_to_ECJ_Dec_trend():
    colums = set()
    for e1 in this.COLUMS_Ime_Currency_VAR_Info:
        s1 = e1.split('__')
        for e2 in this.COLUMS_Ime_Currency_VAR_Info:
            s2 = e2.split('__')
            if s1[0] == s2[0] and s1[1] == s2[1] and s1[-1].startswith('GC_decision_year') and s2[-1].startswith('ECJ_decision_year'):
                if s1[0] in ['Dividend_type', 'Div_pay', 'Div_pay_date']:
                    continue
                colum_name = f'{s1[0]}__{s1[1]}__EC_Dec_to_GC_Dec_trend'
                colums.add(colum_name)
                for row in db.core:
                    v1 = row.get(e1, None)
                    v2 = row.get(e2, None)
                    if None not in [v2, v1] and 'NA' not in [v2, v1]:
                        if float(v2) != 0:
                            row[colum_name] = float(v1)/float(v2)
    for c in colums:
        db.core_fields.append(c)
