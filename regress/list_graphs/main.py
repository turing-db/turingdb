from pytester import TuringdbTester

tester = TuringdbTester()
tester.spawn()
t = tester.client()

# List an empty list of graphs
res = t.query('LIST GRAPH')
print(res)

# Create a graph
res = t.query('CREATE GRAPH mygraph')
print(res)

t.set_graph('mygraph')
df = t.query('CALL db.history()')
print(df)

print('* list_graphs: done')
