from src import db, utils, domain

db.init()
# db.delete_all()
# db.create_nodes()
# db.create_relationships()

c1 = 0
s = len(db.rows)
for i, row in enumerate(db.rows):
    print(round(i/s * 100))
    res = utils.getCoordinates(row.firm.Incorporation_state, row.firm.Firm_address)
    if res.confidence == 1:
        c1+=1

print(c1/len(db.rows) * 100)

