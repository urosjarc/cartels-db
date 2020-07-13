from py2neo import Graph, Relationship, Cursor
import sys
import csv
from src import utils
from src import auth as aut
from src.domain import *
from typing import List

this = sys.modules[__name__]
this.graph = None

this.rows: List[CSVRow] = []
this.stock_meta_rows: List[StockMeta] = []
this.stock_data_rows: List[StockData] = []

this.csvPath = utils.currentDir(__file__, '../data/cartels-db.csv')
this.csvStockPath = utils.currentDir(__file__, '../data/stock-db.csv')
this.csvStockAnnualPath = utils.currentDir(__file__, '../data/stock-annual-eu-db.csv')


# DATABASE
def init():
    this.graph = Graph(uri=aut.dbUrl, auth=aut.neo4j, max_connection=3600 * 24 * 30, keep_alive=True)

    with open(this.csvPath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.rows.append(CSVRow(row))

    with open(this.csvStockPath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.stock_meta_rows.append(StockMeta(row))

    stock_data_rows = {}
    with open(this.csvStockAnnualPath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            sa = StockData(row)

            if sa.StockData is None:
                continue

            if sa.StockData not in stock_data_rows:
                stock_data_rows[sa.StockData] = [sa]
            else:
                stock_data_rows[sa.StockData].append(sa)

        # Merganje propertijev
        for k, vs in stock_data_rows.items():
            SA = vs[0]
            for i in range(1, len(vs)):
                for attr, val in vs[i].__dict__.items():
                    if '_year' in attr or '_stock' in attr:
                        setattr(SA, attr, val)

            SA._init()
            this.stock_data_rows.append(SA)


# DELETE ALL IN DATABASE
def delete_all():
    this.graph.delete_all()


# CREATE NODES
def create_nodes_core():
    size = len(this.rows)
    notExists = {
        'firm': 0,
        'case': 0,
        'holding': 0,
        'undertaking': 0,
    }

    for i, row in enumerate(this.rows):
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


# CREATE CONNECTIONS
def create_relationships_core():
    size = len(this.rows)

    for i, row in enumerate(this.rows):
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
    for row in this.rows:
        try:
            tickers.add(row.firm.Ticker_firm.split(':')[1])
        except:
            pass
    return list(tickers)
