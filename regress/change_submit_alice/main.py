import turingdb

t = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')

# Create a graph
res = t.query('CREATE GRAPH mygraph')
print(res)

# Create a change
t.set_graph('mygraph')
change = t.query('CHANGE NEW')['changeID'][0]

t.checkout(change=str(change))
t.query("CREATE (:Person {name: 'Alice'})")
t.query("COMMIT")
t.query("CHANGE SUBMIT")

print('* change_submit_alice: done')
