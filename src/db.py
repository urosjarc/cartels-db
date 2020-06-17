from py2neo import Graph, Relationship
import sys
import csv
from src import utils
from src.domain import *
from typing import List

this = sys.modules[__name__]
this.graph = None
this.rows: List[CSVRow] = []
this.csvPath = utils.currentDir(__file__, '../data/cartels-db.csv')
this.rel = Relationship.type("RELATIONSHIP")


# DATABASE
def init():
    this.graph = Graph(auth=("neo4j", "urosjarc"), host="localhost", port=7687)

    with open(this.csvPath) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            this.rows.append(CSVRow(row))


# DELETE ALL IN DATABASE
def delete_all():
    this.graph.delete_all()


# CREATE NODES
def create_nodes():
    size = len(this.rows)
    notExists = {
        'firm': 0,
        'case': 0,
        'holding': 0,
        'undertaking': 0,
    }
    for i, row in enumerate(this.rows):
        if i % 100 == 0:
            print(f'CREATING: {round(i / size * 100)}%')

        if row.firm._exists:
            this.graph.merge(row.firm._instance, 'Firm', 'Firm')
        if row.case._exists:
            this.graph.merge(row.case._instance, 'Case', 'Case')
        if row.holding._exists:
            this.graph.merge(row.holding._instance, 'Holding', 'Holding')
        if row.undertaking._exists:
            this.graph.merge(row.undertaking._instance, 'Undertaking', 'Undertaking')


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

def get_firm_tickers():
    tickers = set()
    for row in this.rows:
        try:
            tickers.add(row.firm.Ticker_firm.split(':')[1])
        except:
            pass
    return list(tickers)
