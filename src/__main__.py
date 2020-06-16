from py2neo import Graph, Relationship
import csv
from src import utils
from src.domain import *
from typing import List

# DATABASE
delete = False
graph = Graph(auth=("neo4j", "urosjarc"), host="localhost", port=7687)

if delete:
    graph.delete_all()

# INIT
rows: List[CSVRow] = []
csvPath = utils.currentDir(__file__, '../data/cartels-db.csv')
with open(csvPath) as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        rows.append(CSVRow(row))

# CREATE NODES
size = len(rows)
if delete:
    for i, row in enumerate(rows):
        if i % 50 == 0:
            print(f'CREATING: {round(i / size * 100)}%')
        graph.merge(row.firm._instance, 'Firm', 'Firm')
        graph.merge(row.case._instance, 'Case', 'Case')
        graph.merge(row.holding._instance, 'Holding', 'Holding')
        graph.merge(row.undertaking._instance, 'Undertaking', 'Undertaking')

# CREATE CONNECTIONS
CONNECTION = Relationship.type("CONNECTION")
for i, row in enumerate(rows):
    if i % 50 == 0:
        print(f'CONNECTING: {round(i / size * 100)}%')

    # Firm => Case
    graph.run('MATCH (c:Case),(f:Firm) WHERE c.Case = $Case AND f.Firm = $Firm MERGE (f)-[r:REL]->(c) RETURN type(r)',
              Case = row.case.Case, Firm = row.firm.Firm)

    # Firm => Undertaking
    graph.run('MATCH (f:Firm),(u:Undertaking) WHERE f.Firm = $Firm AND u.Undertaking = $Undertaking MERGE (f)-[r:REL]->(u) RETURN type(r)',
              Firm = row.firm.Firm, Undertaking = row.undertaking.Undertaking)

    # Undertaking => Holding
    graph.run('MATCH (u:Undertaking),(h:Holding) WHERE u.Undertaking = $Undertaking AND h.Holding = $Holding MERGE (u)-[r:REL]->(h) RETURN type(r)',
              Undertaking = row.undertaking.Undertaking, Holding = row.holding.Holding)
