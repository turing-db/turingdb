from turingdb import TuringClient

client = TuringClient(host="http://localhost:6666")
graph = "test_graph4"
client.query(f"CREATE GRAPH {graph}")
client.set_graph(graph)

for i in range(20):
    change = client.new_change()

    client.checkout(change=change)
    print(f"Submitting change {change}")
    client.query("CHANGE SUBMIT")
    client.checkout()
