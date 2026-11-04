import turingdb

t = turingdb.TuringDB(host="localhost")

# Create a graph
res = t.query("CREATE GRAPH mygraph")
print(res)

# Create a change
t.set_graph("mygraph")
change = t.new_change()

t.checkout(change=change)
t.query("CREATE (:Person {name: 'Alice'})")
t.query("COMMIT")
t.query("CHANGE SUBMIT")

print("* change_submit_alice: done")
