import turingdb

t = turingdb.TuringDB(instance_id="", auth_token="", host="http://localhost:6666")

print("Running Wine Ontology Test")

# Create a graph
res = t.query("CREATE GRAPH mygraph")

# Create a change
t.set_graph("mygraph")
change = t.query("CHANGE NEW")["changeID"][0]

t.checkout(change=str(change))

# Try build the Wine Ontology graph which has too many labels
with open('wine_ontology.cypher') as f:
  wine_create_query = f.read()

try:
  res = t.query(wine_create_query)
except Exception as e:
  assert str(e) == "EXEC_ERROR: Unexpected exception: Attempted to create LabelID 256, which exceeds graph label capacity."
else:
  assert False # If an exception wasn't raised: bug


print("Passed Wine Ontology Test")
