import turingdb

t = turingdb.TuringClient()
t.try_reach()

# Create a change
t.set_graph("default")
change = t.new_change()

# Submit change
t.checkout(change=change)
print("Submit change " + str(change))
t.query("CHANGE SUBMIT")

print("* submit_empty_change: done")
