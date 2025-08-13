import turingdb

t = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')

# List an empty list of graphs
res = t.query('LIST GRAPH')
print(res)

# Create a graph
res = t.query('CREATE GRAPH mygraph')
print(res)

print('* list_graphs: done')
