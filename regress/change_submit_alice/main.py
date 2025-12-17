from pytester import TuringdbTester

tester = TuringdbTester()
tester.spawn()
t = tester.client()

# Create a graph
res = t.query('CREATE GRAPH mygraph')
print(res)

# Create a change
t.set_graph('mygraph')
change = t.query('CHANGE NEW')["changeID"][0]

t.checkout(change=str(change))
#t.query("CREATE (:Person {name: 'Alice'})") # FIXME
t.query("CREATE (:Person)")
t.query("COMMIT")
t.query("CHANGE SUBMIT")

print('* change_submit_alice: done')
