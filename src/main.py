from py2neo import Graph, Node, Relationship

graph = Graph(auth=("neo4j", "urosjarc"), host="localhost", port=7687)

a = Node("Person", name="Uros", age=53)
b = Node("Person", name="Lol", age=64)
KNOWS = Relationship.type("KNOWS")
graph.merge(KNOWS(a, b, since=1993), "Person", "name")