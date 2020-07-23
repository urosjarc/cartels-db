from src import db


# INIT
db.init()
db.delete_all()

# CREATE NODES
db.create_nodes_core()
db.create_nodes_stock_meta()
db.create_nodes_stock_data()
db.create_nodes_stock_data_others()

db.report()


# CREATE RELATIONSHIPS
# db.create_relationships_core()
# db.create_relationships_stock_meta()
# db.create_relationships_stock_data()
