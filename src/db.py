import sys
import csv
from src import utils
from typing import List, Dict

this = sys.modules[__name__]

this.stock_meta_rows = []
this.A1012M_EU_rows = {}
this.A1012M_LOCAL_rows = {}

# INPUT
this.csvCorePathIn = utils.currentDir(__file__, '../data/csv/core.csv')
this.csv_EC_annual_data = utils.currentDir(__file__, '../data/csv/core_EC_annual_data.csv')
this.csv_ECJ_annual_data = utils.currentDir(__file__, '../data/csv/core_ECJ_annual_data.csv')
this.csvStockMetaPath = utils.currentDir(__file__, '../data/csv/stock-meta.csv')
this.csvStockDataAnnualPath = utils.currentDir(__file__, '../data/csv/A1012M/')

# OUTPUT
this.csvCorePathOut = utils.currentDir(__file__, '../data/csv/core_out_tickers.csv')
this.csvAnnualPathOut = utils.currentDir(__file__, '../data/csv/')

# DATABASE
core = None
core_fields = None
core_EC_annual_data: List[Dict] = []
core_ECJ_annual_data: List[Dict] = []

this.core: List[Dict] = []
this.core_EC_annual_data: List[Dict] = []
this.core_ECJ_annual_data: List[Dict] = []
this.core_fields: List[str] = []


def init_core():
    with open(this.csv_EC_annual_data, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.core_EC_annual_data.append(row)

    with open(this.csv_ECJ_annual_data, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.core_ECJ_annual_data.append(row)

    with open(this.csvCorePathIn, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.core.append(row)

    this.core_fields = list(this.core[0].keys())


def init_nodes_stock_meta():
    with open(this.csvStockMetaPath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # this.stock_meta_rows.append(StockMeta(row))
            this.stock_meta_rows.append(row)


def init_nodes_annual(dir, saved_one=True):
    this.A1012M_EU_rows = {}
    this.A1012M_LOCAL_rows = {}

    csvFile1 = None
    if saved_one:
        csvFile1 = open(this.csvStockDataAnnualPath + dir + '/annual_figures_eu_1.csv')
    with open(this.csvStockDataAnnualPath + dir + '/annual_figures_eu.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        if saved_one:
            reader1 = csv.DictReader(csvFile1)
        li = []

        for row in reader:
            li.append(row)
        if saved_one:
            for row1 in reader1:
                li.append(row1)

        for l in li:
            if l['Name'] == '#ERROR':
                continue
            ticker = utils.getCode(l['Code'])
            if ticker not in this.A1012M_EU_rows:
                this.A1012M_EU_rows[ticker] = [l]
            else:
                this.A1012M_EU_rows[ticker].append(l)

    if saved_one:
        csvFile1 = open(this.csvStockDataAnnualPath + dir + 'annual_figures_local_1.csv')
    with open(this.csvStockDataAnnualPath + dir + 'annual_figures_local.csv') as csvfile:
        reader = csv.DictReader(csvfile)
        if saved_one:
            reader1 = csv.DictReader(csvFile1)
        li = []
        for row in reader:
            li.append(row)
        if saved_one:
            for row1 in reader1:
                li.append(row1)
        for l in li:
            if l['Name'] == '#ERROR':
                continue
            ticker = utils.getCode(l['Code'])
            if ticker not in this.A1012M_LOCAL_rows:
                this.A1012M_LOCAL_rows[ticker] = [l]
            else:
                this.A1012M_LOCAL_rows[ticker].append(l)


def save_core():
    with open(this.csvCorePathOut, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=this.core_fields)
        writer.writeheader()
        writer.writerows(this.core)


def save_annual():
    with open(this.csvAnnualPathOut + '/annual_figures_eu.csv', 'w') as csvfile:
        l = []
        for k, rows in this.A1012M_EU_rows.items():
            for row in rows:
                l.append(row)

        writer = csv.DictWriter(csvfile, fieldnames=l[0].keys())
        writer.writeheader()
        writer.writerows(l)

    with open(this.csvAnnualPathOut + '/annual_figures_local.csv', 'w') as csvfile:
        l = []
        for k, rows in this.A1012M_LOCAL_rows.items():
            for row in rows:
                l.append(row)

        writer = csv.DictWriter(csvfile, fieldnames=l[0].keys())
        writer.writeheader()
        writer.writerows(l)
