from src import db #, utils, domain

db.init()
db.delete_all()
db.create_nodes()
db.create_relationships()

