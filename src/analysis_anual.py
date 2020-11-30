from src import db, utils

"""
Nove spremenljivke annual
"""

def Current_ratio():
    for rows_group in [db.A1012M_EU_rows, db.A1012M_LOCAL_rows]:
        for ticker, rows in rows_group.items():
            found = False
            for row in rows:
                for row1 in rows:
                    if 'CURRENT ASSETS - TOTAL' in row['Name'] and 'CURRENT LIABILITIES' in row1['Name']:
                        new_row = utils.create_annual_row('CURRENT RATIO', row)
                        for year in utils.get_annual_years(row):
                            try:
                                new_row[year] = float(row[year]) - float(row1[year])
                            except Exception:
                                new_row[year] = 'NA'

                        rows_group[ticker].append(new_row)
                        found = True
                        break
                if found:
                    break

def Acid_test_ratio():
    for rows_group in [db.A1012M_EU_rows, db.A1012M_LOCAL_rows]:
        for ticker, rows in rows_group.items():
            found = False
            for row in rows:
                for row1 in rows:
                    for row2 in rows:
                        if \
                                'CURRENT ASSETS - TOTAL' in row['Name'] and\
                                'CURRENT LIABILITIES' in row1['Name'] and\
                                'TOTAL INVENTORIES' in row2['Name']:

                            new_row = utils.create_annual_row('ACID TEST RATIO', row)
                            for year in utils.get_annual_years(row):
                                try:
                                    new_row[year] = (float(row[year]) - float(row2[year]))/float(row1[year])
                                except Exception:
                                    new_row[year] = 'NA'

                            rows_group[ticker].append(new_row)
                            found = True
                            break
                    if found:
                        break
                if found:
                    break

def Cash_ratio():
    for rows_group in [db.A1012M_EU_rows, db.A1012M_LOCAL_rows]:
        for ticker, rows in rows_group.items():
            found = False
            for row in rows:
                for row1 in rows:
                    if 'CASH' in row['Name'] and 'CURRENT LIABILITIES' in row1['Name']:
                        new_row = utils.create_annual_row('CASH RATIO', row)
                        for year in utils.get_annual_years(row):
                            try:
                                new_row[year] = float(row[year]) / float(row1[year])
                            except Exception:
                                new_row[year] = 'NA'

                        rows_group[ticker].append(new_row)
                        found = True
                        break
                if found:
                    break

def Debt_ratio():
    for rows_group in [db.A1012M_EU_rows, db.A1012M_LOCAL_rows]:
        for ticker, rows in rows_group.items():
            found = False
            for row in rows:
                for row1 in rows:
                    if 'TOTAL LIABILITIES' in row['Name'] and 'TOTAL ASSETS' in row1['Name']:
                        new_row = utils.create_annual_row('DEBT RATIO', row)
                        for year in utils.get_annual_years(row):
                            try:
                                new_row[year] = float(row[year]) / float(row1[year])
                            except Exception:
                                new_row[year] = 'NA'

                        rows_group[ticker].append(new_row)
                        found = True
                        break
                if found:
                    break

def Debt_to_equity_ratio():
    for rows_group in [db.A1012M_EU_rows, db.A1012M_LOCAL_rows]:
        for ticker, rows in rows_group.items():
            found = False
            for row in rows:
                for row1 in rows:
                    if 'TOTAL LIABILITIES' in row['Name'] and "COMMON SHAREHOLDERS EQUITY" in row1['Name']:
                        new_row = utils.create_annual_row('DEBT TO EQUITY RATIO', row)
                        for year in utils.get_annual_years(row):
                            try:
                                new_row[year] = float(row[year]) / float(row1[year])
                            except Exception:
                                new_row[year] = 'NA'

                        rows_group[ticker].append(new_row)
                        found = True
                        break
                if found:
                    break

def Equity_ratio():
    for rows_group in [db.A1012M_EU_rows, db.A1012M_LOCAL_rows]:
        for ticker, rows in rows_group.items():
            found = False
            for row in rows:
                for row1 in rows:
                    if "COMMON SHAREHOLDERS EQUITY" in row['Name'] and "TOTAL ASSETS" in row1['Name']:
                        new_row = utils.create_annual_row('EQUITY RATIO', row)
                        for year in utils.get_annual_years(row):
                            try:
                                new_row[year] = float(row[year]) / float(row1[year])
                            except Exception:
                                new_row[year] = 'NA'

                        rows_group[ticker].append(new_row)
                        found = True
                        break
                if found:
                    break