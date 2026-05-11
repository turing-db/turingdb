from turingdb import TuringClient

client = TuringClient(host="http://localhost:6666")
graph = "test_history_on_change"
client.query(f"CREATE GRAPH {graph}")
client.set_graph(graph)

# Submit a change so we have some commit history
change = client.new_change()
client.checkout(change=change)
client.query("CREATE (:Person {name: 'Alice'})")
client.query("CHANGE SUBMIT")
client.checkout()

# Verify history works on main
df = client.query("CALL db.history()")
print("History on main:")
print(df)
assert df.shape[0] >= 2, f"Expected at least 2 commits on main, got {df.shape[0]}"

# Create a new change and check out onto it without committing
change = client.new_change()
client.checkout(change=change)

# This is the bug from issue #521: db.history() crashes on a change
df = client.query("CALL db.history()")
print("History on change:")
print(df)
assert df.shape[0] >= 2, f"Expected at least 2 commits on change, got {df.shape[0]}"

print("PASSED")
