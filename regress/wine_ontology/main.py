import turingdb

t = turingdb.TuringDB(instance_id="", auth_token="", host="http://localhost:6666")

# Create a graph
res = t.query("CREATE GRAPH mygraph")
print(res)

# Create a change
t.set_graph("mygraph")
change = t.query("CHANGE NEW")["changeID"][0]

t.checkout(change=str(change))

# Query
with open('wine_ontology.cypher') as f: wine_create_query = f.read()
res = t.query(wine_create_query)
