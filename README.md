# Dependencies

```
sudo pip3 install py2neo
sudo apt install neo4j
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