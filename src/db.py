import sys
from os import listdir
from os.path import isfile, join
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
this.csvPath = utils.currentDir(__file__, '../data/csv/')

# OUTPUT
this.csvCorePathOut = utils.currentDir(__file__, '../data/csv/core_out_tickers.csv')
this.csvMarketIndicesOut = utils.currentDir(__file__, '../data/csv/core_market_indices.csv')
this.csvAnnualPathOut = utils.currentDir(__file__, '../data/csv/')

# DATABASE
core = None
core_fields = None
core_EC_annual_data: List[Dict] = []
core_ECJ_annual_data: List[Dict] = []
core_market_indices: Dict[str,Dict[str,list]] = {}

this.core: List[Dict] = []
this.core_EC_annual_data: List[Dict] = []
this.core_ECJ_annual_data: List[Dict] = []
this.core_fields: List[str] = []

this.core_A1012M_local: dict = {}
this.core_A1012M_euro: dict = {}
this.core_A1012M_all_euro = []
this.core_A1012M_all_local = []

this.core_market_indices = {}
this.core_market_indices_all = []

this.names_A1012M = set()


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


def init_nodes_A1012M():
    type = input("Vnesi tip A1012M [local/euro]: ")
    local_path = this.csvStockDataAnnualPath + f'/data/local'
    euro_path = this.csvStockDataAnnualPath + f'/data/euro'

    local_files = [f for f in listdir(local_path) if isfile(join(local_path, f))]
    euro_files = [f for f in listdir(euro_path) if isfile(join(euro_path, f))]

    if type == 'local':
        for n in local_files:
            name = n.split(", ")[1].replace(' ', '_')
            this.names_A1012M.add(name)
            with open(this.csvStockDataAnnualPath + f'/data/local/{n}', 'r', errors='ignore') as csvfile:
                print(csvfile.name)
                reader = csv.DictReader(csvfile)
                this.core_A1012M_local[name] = []
                for row in reader:
                    this.core_A1012M_local[name].append(row)

    if type == 'euro':
        for n in euro_files:
            name = n.split(", ")[1].replace(' ', '_')
            this.names_A1012M.add(name)
            with open(this.csvStockDataAnnualPath + f'/data/euro/{n}', 'r', errors='ignore') as csvfile:
                reader = csv.DictReader(csvfile)
                this.core_A1012M_euro[name] = []
                for row in reader:
                    this.core_A1012M_euro[name].append(row)

    return A1012M_type

def init_nodes_market_indices():
    paths = [
        this.csvPath + f'/LEV/2IN',
        this.csvPath + f'/LEV/4SE',
        this.csvPath + f'/DSLOC',
        this.csvPath + f'/MLOC',
        this.csvPath + f'/TOTMKWD',
    ]
    for path in paths:
        files_paths = [f for f in listdir(path) if isfile(join(path, f))]
        dicts = {}
        for file_path in files_paths:
            name = file_path.split(", ")[1].replace(' ', '_')
            with open(f'{path}/{file_path}', 'r', errors='ignore') as csvfile:
                print(csvfile.name)
                reader = csv.DictReader(csvfile)
                dicts[name] = []
                for row in reader:
                    dicts[name].append(row)
        this.core_market_indices[path.split('/')[-1]] = dicts

def save_A1012M(type):
    rows_local = this.core_A1012M_all_local
    rows_euro = this.core_A1012M_all_euro
    if type == 'local':
        with open(this.csvStockDataAnnualPath + f'/../A1012M_local.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=rows_local[0].keys())
            writer.writeheader()
            writer.writerows(rows_local)

    if type == 'euro':
        with open(this.csvStockDataAnnualPath + f'/../A1012M_euro.csv', 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=rows_euro[0].keys())
            writer.writeheader()
            writer.writerows(rows_euro)


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


def save_market_indices():
    with open(this.csvMarketIndicesOut, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=this.core_market_indices_all[0].keys())
        writer.writeheader()
        writer.writerows(this.core_market_indices_all)
