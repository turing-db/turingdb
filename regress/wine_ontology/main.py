import turingdb

t = turingdb.TuringClient()
t.try_reach()

print("Running Wine Ontology Test")

# Create a graph
res = t.query("CREATE GRAPH mygraph")

# Create a change
t.set_graph("mygraph")
change: int = t.new_change()

t.checkout(change=change)

# Try build the Wine Ontology graph which has too many labels
with open("wine_ontology.cypher") as f:
    wine_create_query = f.read()

try:
    res = t.query(wine_create_query)
except Exception as e:
    assert (
        str(e)
        == "EXEC_ERROR: Attempted to create LabelID 256, which exceeds graph label capacity."
    )
else:
    assert False  # If an exception wasn't raised: bug


print("Passed Wine Ontology Test")
