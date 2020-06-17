from src import db, stock

db.init()
# db.delete_all()
# db.create_nodes()
# db.create_relationships()

ticker = db.get_firm_tickers()
stock.plotStock(ticker[3])