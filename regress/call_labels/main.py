import turingdb

t = turingdb.TuringDB(instance_id='', auth_token='', host='http://localhost:6666')

# Create a graph
res = t.query('CREATE GRAPH mygraph')
print(res)

# Create a change
t.set_graph('mygraph')
change = t.query('CHANGE NEW')[0][0]

t.checkout(change=str(change))
t.query("CREATE (:Person {name: 'Alice'})")
t.query("CREATE (:Person {name: 'Bob'})")
t.query("CREATE (:Person {name: 'Charlie'})")
t.query("CREATE (:Person, Employee {name: 'Dave'})")
t.query("CREATE (:Person, Employee {name: 'Eve'})")
t.query("CREATE (:Person, Employee {name: 'Mallory'})")
t.query("CREATE (:Person, Employee {name: 'Trent'})")
t.query("CREATE (:Person, Employee {name: 'Wendy'})")
t.query("COMMIT")
t.query("CHANGE SUBMIT")

# Go on to main
t.checkout('main')
df = t.query('CALL LABELS()')
print(df)
