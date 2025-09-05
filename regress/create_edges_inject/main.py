import turingdb

t = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')

# Create a graph
res = t.query('CREATE GRAPH mygraph')
print(res)

# Build graph
t.set_graph('mygraph')
change = t.query('CHANGE NEW')['Change ID'][0]

t.checkout(change=str(change))
t.query("CREATE (:Person)")
t.query("CREATE (:Person)")
t.query("CREATE (:Person)")
t.query("COMMIT")
t.query("CHANGE SUBMIT")

# Create edge by injecting node IDs
t.checkout('head')
change = t.query('CHANGE NEW')['Change ID'][0]
t.checkout(change=str(change))
t.query('create (n @ 0)-[:GOODEDGE]-(m @ 1)-[:GOODEDGE]-(o @ 2)')
t.query('change submit')

print('* create_edges_inject: done')
