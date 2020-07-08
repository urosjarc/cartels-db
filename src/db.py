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
this.stock_rows: List[Stock] = []

this.csvPath = utils.currentDir(__file__, '../data/cartels-db.csv')
this.csvStockPath = utils.currentDir(__file__, '../data/stock-db.csv')

# DATABASE
def init():
    this.graph = Graph(uri=aut.dbUrl, auth=aut.neo4j, max_connection=3600*24*30, keep_alive=True)

    with open(this.csvPath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.rows.append(CSVRow(row))

    with open(this.csvStockPath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.stock_rows.append(Stock(row))


# DELETE ALL IN DATABASE
def delete_all():
    this.graph.delete_all()


# CREATE NODES
def create_nodes():
    size = len(this.rows)
    size_stock = len(this.stock_rows)
    notExists = {
        'firm': 0,
        'case': 0,
        'holding': 0,
        'undertaking': 0,
    }

    for i, row in enumerate(this.stock_rows):
        if i % 100 == 0:
            print(f'CREATING STOCK: {round(i / size_stock * 100)}%')

        this.graph.merge(row._instance, 'Stock', 'Type')

    for i, row in enumerate(this.rows):
        if i % 100 == 0:
            print(f'CREATING: {round(i / size * 100)}%')

        if row.firm._exists:
            this.graph.merge(row.firm._instance, 'Firm', 'Firm')
        if row.case._exists:
            this.graph.merge(row.case._instance, 'Case', 'Case')
        if row.holding._exists:
            this.graph.merge(row.holding._instance, 'Holding', 'Holding')
        else:
            notExists['holding']+=1
        if row.undertaking._exists:
            this.graph.merge(row.undertaking._instance, 'Undertaking', 'Undertaking')
        else:
            notExists['undertaking']+=1

    print(notExists)

# CREATE CONNECTIONS
def create_relationships():
    size = len(this.rows)

    for i, row in enumerate(this.rows):
        if i % 100 == 0:
            print(f'CONNECTING: {round(i / size * 100)}%')

        # Firm => Case
        if row.firm._exists and row.case._exists:
            this.graph.run(
                'MATCH (c:Case),(f:Firm) WHERE c.Case = $Case AND f.Firm = $Firm MERGE (f)-[r:REL]->(c) RETURN type(r)',
                Case=row.case.Case, Firm=row.firm.Firm)
        else:
            raise Exception(f'Not exists: {row.firm} {row.case}')

        # Firm => Undertaking
        if row.firm._exists and row.undertaking._exists:
            this.graph.run(
                'MATCH (f:Firm),(u:Undertaking) WHERE f.Firm = $Firm AND u.Undertaking = $Undertaking MERGE (f)-[r:REL]->(u) RETURN type(r)',
                Firm=row.firm.Firm, Undertaking=row.undertaking.Undertaking)
        else:
            raise Exception(f'Not exists: {row.firm} {row.undertaking}')

        # Undertaking => Holding
        if row.undertaking._exists and row.holding._exists:
            this.graph.run(
                'MATCH (u:Undertaking),(h:Holding) WHERE u.Undertaking = $Undertaking AND h.Holding = $Holding MERGE (u)-[r:REL]->(h) RETURN type(r)',
                Undertaking=row.undertaking.Undertaking, Holding=row.holding.Holding)

def create_relationships_stock():
    size_stock = len(this.stock_rows)
    for i, row in enumerate(this.stock_rows):
        if i % 100 == 0:
            print(f'CONNECTING STOCK: {round(i / size_stock * 100)}%')

        rel: Cursor = this.graph.run(
            'MATCH (s:Stock),(f:Firm) WHERE s.Type = $Ticker_firm AND f.Ticker_firm = $Ticker_firm MERGE (s)-[r:REL_STOCK]->(f) RETURN type(r)',
            Ticker_firm=row.Type)
        print(rel.to_table())
        this.graph.run(
            'MATCH (s:Stock),(u:Undertaking) WHERE s.Type = $Ticker_firm AND u.Ticker_undertaking = $Ticker_firm MERGE (s)-[r:REL_STOCK]->(u) RETURN type(r)',
            Ticker_firm=row.Type)
        this.graph.run(
            'MATCH (s:Stock),(h:Holding) WHERE s.Type = $Ticker_firm AND h.Holding_Ticker_parent= $Ticker_firm MERGE (s)-[r:REL_STOCK]->(h) RETURN type(r)',
            Ticker_firm=row.Type)

def get_firm_tickers():
    tickers = set()
    for row in this.rows:
        try:
            tickers.add(row.firm.Ticker_firm.split(':')[1])
        except:
            pass
    return list(tickers)
