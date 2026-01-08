import turingdb

t = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')

# Create a graph
res = t.query('CREATE GRAPH mygraph')
print(res)

# Build graph
t.set_graph('mygraph')
change = t.query('CHANGE NEW')['changeID'][0]

t.checkout(change=str(change))
t.query("CREATE (:Person {id:0})")
t.query("CREATE (:Person {id:1})")
t.query("CREATE (:Person {id:2})")
t.query("COMMIT")
t.query("CHANGE SUBMIT")

# Create edge by injecting node IDs
t.checkout('head')
change = t.query('CHANGE NEW')['changeID'][0]
t.checkout(change=str(change))
t.query('MATCH (n), (m), (o) WHERE n.id = 0 AND m.id = 1 AND o.id = 2 CREATE (n)-[:GOODEDGE]->(m)-[:GOODEDGE]->(o)')
t.query('CHANGE SUBMIT')

print('* create_edges_inject: done')
