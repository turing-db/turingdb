import turingdb

t = turingdb.TuringDB(host='localhost')

# List an empty list of graphs
res = t.query('LIST GRAPH')
print(res)

# Create a graph
res = t.query('CREATE GRAPH mygraph')
print(res)

print('* list_graphs: done')
