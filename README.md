# Dependencies

```
sudo pip3 install py2neo==5.0b5
sudo apt install neo4j
sudo pip3 install pycountry
```

# Starting DB

```
sudo neo4j start
```

# Exporting DB
```
sudo neo4j-admin dump --database=neo4j --verbose --to=/home/urosjarc/cartels.db
```

# Importing DB
```
sudo neo4j-admin load --verbose --from=/home/urosjarc/cartels.db --database=neo4j --force --info
```

# Test DB
```
pass: VvipLE4UzMRYfW1G
user: neo4j
url: https://34-66-123-143.gcp-neo4j-sandbox.com:7473/
```

# Will do...

1. Import to db from stock_data_fix.
2. Connect all from stock-data.
3. Create additional nodes from stock_info
4. Connect them all "povezovalni element" iz "Datastream podatki..."
5. Potem sledi "Navodila za obdelavo podatkov..." glej stolpec
F[Prepis/obdelava v isti stolpec] in G[Nove spremenljivke ->] in nadalni stolpci 

