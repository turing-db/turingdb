import turingdb

t = turingdb.TuringClient(host="http://localhost:6666")

# Create a graph
res = t.query("CREATE GRAPH mygraph")
print(res)

# Build graph
t.set_graph("mygraph")
change = t.new_change()

t.checkout(change=change)
t.query("CREATE (:Person {id:0})")
t.query("CREATE (:Person {id:1})")
t.query("CREATE (:Person {id:2})")
t.query("COMMIT")
t.query("CHANGE SUBMIT")

# Create edge by injecting node IDs
t.checkout()
change = t.new_change()
t.checkout(change=change)
t.query(
    "MATCH (n), (m), (o) WHERE n.id = 0 AND m.id = 1 AND o.id = 2 CREATE (n)-[:GOODEDGE]->(m)-[:GOODEDGE]->(o)"
)
t.query("CHANGE SUBMIT")

print("* create_edges_inject: done")
