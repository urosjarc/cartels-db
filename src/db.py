from py2neo import Graph
import sys
import csv
from src import utils
from src import auth as aut
from src.domain import *
from typing import List
import os

this = sys.modules[__name__]
this.graph = None

this.core_rows: List[CSV_Core] = []
this.stock_meta_rows: List[StockMeta] = []
this.stock_data_rows: List[StockData] = []

this.csvCorePath = utils.currentDir(__file__, '../data/csv/core.csv')
this.csvStockMetaPath = utils.currentDir(__file__, '../data/csv/stock-meta.csv')
this.csvStockDataPaths = [i for i in utils.absoluteFilePaths(utils.currentDir(__file__, '../data/csv/A1012M/data'))]
this.csvStockDataAnnualPath = utils.currentDir(__file__, '../data/csv/A1012M/annual_figures.csv')

# DATABASE

def init():
    init_db()
    # init_nodes_core()
    # init_nodes_stock_meta()
    init_nodes_stock_data_A1012M()
    init_nodes_stock_data_A1012M_annual()

def init_db():
    this.graph = Graph(uri=aut.dbUrl, auth=aut.neo4j, max_connection=3600 * 24 * 30, keep_alive=True)

def init_nodes_core():
    with open(this.csvCorePath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.core_rows.append(CSV_Core(row))

def init_nodes_stock_meta():
    with open(this.csvStockMetaPath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.stock_meta_rows.append(StockMeta(row))

def init_nodes_stock_data_A1012M():
    # GET ALL FILES
    stockDatas = {} # code -> path -> StockData
    names = []
    dates = {}
    for path in this.csvStockDataPaths:
        name = path.split("/")[-1].split(',')[1][1:].replace(' ', '_')
        print(f'Parsing: [{name + "]":<62}')

        names.append(name)
        dates[name] = None

        sds = StockData.parse_A1012M(path)

        # MERGE DATA
        for sd in sds:
            if dates[name] is None:
                dates[name] = sd.dates

            if sd.StockData in [None, '']:
                continue

            if sd.StockData not in stockDatas:
                setattr(sd, name, sd.values)
                setattr(sd, name+"_dates", sd.dates)
                delattr(sd, 'values')
                delattr(sd, 'dates')
                stockDatas[sd.StockData] = sd
            else:
                nameVal = getattr(stockDatas[sd.StockData], name, None)
                if nameVal is not None:
                    raise Exception(f'{name} is occuring multiple times for {sd.StockData}')
                setattr(stockDatas[sd.StockData], name, sd.values)
                setattr(stockDatas[sd.StockData], name+'_dates', sd.dates)

    # Adding missing properties
    missingVal = 0
    vals = 0
    for code, stockData in stockDatas.items():
        for n in names:
            data = getattr(stockData, n, None)

            if data is None:
                zerros = [-1 for i in dates[n]]
                setattr(stockData, n, zerros)
                setattr(stockData, n+"_dates", dates[n])
                missingVal+= 1
            else:
                vals+=1
    # Initing

    for sd in stockDatas.values():
        this.stock_data_rows.append(sd)

    print(f'\nAttributes added: {round(missingVal/vals * 100, 2)}% out of {vals}')

def init_nodes_stock_data_A1012M_annual():
    stockDatas = {} # code -> name -> StockData
    uniqueNames = set()

    sds = StockData.parse_A1012M_annual(this.csvStockDataAnnualPath)
    sd_date = sds[0].dates
    sd_default = [-1 for d in sd_date]

    for sd in sds:
        name = utils.reformat_value(sd._data['Name'].split(' - ')[-1])
        if name == '#ERROR':
            continue

        name = 'annual_' + name

        uniqueNames.add(name)
        if sd.StockData not in stockDatas:
            stockDatas[sd.StockData] = { name: sd }
        else:
            stockDatas[sd.StockData][name] = sd

    for sd in this.stock_data_rows:
        if sd.StockData in stockDatas:

            setattr(sd, 'annual_dates', sd_date)

            for k, v in stockDatas[sd.StockData].items():
                setattr(sd, k, v.values)

            for un in uniqueNames.difference(list(stockDatas[sd.StockData].keys())):
                setattr(sd, un, sd_default)

            sd._init()

# DELETE ALL IN DATABASE
def delete_all():
    this.graph.delete_all()


# CREATE NODES
def create_nodes_core():
    size = len(this.core_rows)
    notExists = {
        'firm': 0,
        'case': 0,
        'holding': 0,
        'undertaking': 0,
    }

    for i, row in enumerate(this.core_rows):
        if i % 100 == 0:
            print(f'CREATING CORE NODES: {round(i / size * 100)}%')

        if row.firm._exists:
            this.graph.merge(row.firm._instance, 'Firm', 'Firm')
        if row.case._exists:
            this.graph.merge(row.case._instance, 'Case', 'Case')
        if row.holding._exists:
            this.graph.merge(row.holding._instance, 'Holding', 'Holding')
        else:
            notExists['holding'] += 1
        if row.undertaking._exists:
            this.graph.merge(row.undertaking._instance, 'Undertaking', 'Undertaking')
        else:
            notExists['undertaking'] += 1

    print(notExists)


def create_nodes_stock_meta():
    size_stock = len(this.stock_meta_rows)
    for i, row in enumerate(this.stock_meta_rows):
        if i % 100 == 0:
            print(f'CREATING STOCK META NODES: {round(i / size_stock * 100)}%')

        if row._exists:
            this.graph.merge(row._instance, 'StockMeta', 'StockMeta')


def create_nodes_stock_data():
    size = len(this.stock_data_rows)
    for i, row in enumerate(this.stock_data_rows):
        if i % 100 == 0:
            print(f'CREATING STOCK DATA NODES: {round(i / size * 100)}%')

        if row._exists:
            this.graph.merge(row._instance, 'StockData', 'StockData')

# TESTS
def test_nodes_stock_data():
    nansCount = 0

    for filePath in this.csvStockDataPaths:
        if filePath.split('/')[-1] not in StockData.addMissingAttributesFiles():
            with open(filePath) as f:
                nansCount += f.read().count(',NA,')

    nansCountParsed = 0
    for row in this.stock_data_rows:
        for attr, val in row.__dict__.items():
            nansCountParsed += val.values(-1)

    print(nansCount, nansCountParsed, round(nansCountParsed/nansCount * 100, 2))




# CREATE CONNECTIONS
def create_relationships_core():
    size = len(this.core_rows)

    for i, row in enumerate(this.core_rows):
        if i % 100 == 0:
            print(f'CONNECTING CORE: {round(i / size * 100)}%')

        # Firm => Case
        if row.firm._exists and row.case._exists:
            this.graph.run(
                'MATCH (c:Case), (f:Firm) WHERE c.Case=$Case AND f.Firm=$Firm MERGE (f)-[r:REL]->(c) RETURN type(r)',
                Case=row.case.Case, Firm=row.firm.Firm)
        else:
            raise Exception(f'Not exists: {row.firm} {row.case}')

        # Firm => Undertaking
        if row.firm._exists and row.undertaking._exists:
            this.graph.run(
                'MATCH (f:Firm), (u:Undertaking) WHERE f.Firm=$Firm AND u.Undertaking=$Undertaking MERGE (f)-[r:REL]->(u) RETURN type(r)',
                Firm=row.firm.Firm, Undertaking=row.undertaking.Undertaking)
        else:
            raise Exception(f'Not exists: {row.firm} {row.undertaking}')

        # Undertaking => Holding
        if row.undertaking._exists and row.holding._exists:
            this.graph.run(
                'MATCH (u:Undertaking), (h:Holding) WHERE u.Undertaking=$Undertaking AND h.Holding=$Holding MERGE (u)-[r:REL]->(h) RETURN type(r)',
                Undertaking=row.undertaking.Undertaking, Holding=row.holding.Holding)


def create_relationships_stock_meta():
    size = len(this.stock_meta_rows)
    for i, row in enumerate(this.stock_meta_rows):
        if i % 100 == 0:
            print(f'CONNECTING STOCK META: {round(i / size * 100)}%')

        if not row._exists:
            raise Exception(f'Not exists: {row.StockMeta}')

        this.graph.run(
            'MATCH (sm:StockMeta), (f:Firm) WHERE sm.StockMeta=$StockMeta AND f.Stock_exchange_firm=$StockMeta MERGE (sm)-[r:REL_STOCK_META]->(f) RETURN type(r)',
            StockMeta=row.StockMeta)

        this.graph.run(
            'MATCH (sm:StockMeta), (u:Undertaking) WHERE sm.StockMeta=$StockMeta AND u.Stock_exchange_undertaking=$StockMeta MERGE (sm)-[r:REL_STOCK_META]->(u) RETURN type(r)',
            StockMeta=row.StockMeta)

        this.graph.run(
            'MATCH (sm:StockMeta), (h:Holding) WHERE sm.StockMeta=$StockMeta AND h.Stock_exchange_holding=$StockMeta MERGE (sm)-[r:REL_STOCK_META]->(h) RETURN type(r)',
            StockMeta=row.StockMeta)


def create_relationships_stock_data():
    size = len(this.stock_data_rows)
    for i, row in enumerate(this.stock_data_rows):
        if i % 100 == 0:
            print(f'CONNECTING STOCK DATA: {round(i / size * 100)}%')

        if not row._exists:
            raise Exception(f'Not exists: {i} {row.__dict__}')

        this.graph.run(
            'MATCH (sm:StockMeta), (sd:StockData) WHERE sm.StockMeta=$StockData AND sd.StockData=$StockData MERGE (sm)-[r:REL_STOCK_DATA]->(sd) RETURN type(r)',
            StockData=row.StockData)


def get_firm_tickers():
    tickers = set()
    for row in this.core_rows:
        try:
            tickers.add(row.firm.Ticker_firm.split(':')[1])
        except:
            pass
    return list(tickers)
