from src import db

# INIT
db.init()
db.delete_all()

# CREATE NODES
db.create_nodes_core()
db.create_nodes_stock()
db.create_nodes_stock_annual()

# CREATE RELATIONSHIPS
db.create_relationships_core()
db.create_relationships_stock()
db.create_relationships_stock_annual()
