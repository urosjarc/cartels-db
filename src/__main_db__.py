from src import db


# INIT
db.init()
db.delete_all()

# CREATE NODES
db.create_nodes_stock_meta()
db.create_nodes_stock_data()

# CREATE RELATIONSHIPS
db.create_relationships_core()
db.create_relationships_stock_meta()
db.create_relationships_stock_A1012M()
db.create_relationships_stock_LEV2IN()
db.create_relationships_stock_LEV4SE()
db.create_relationships_stock_DSLOC()
db.create_relationships_stock_MLOC()