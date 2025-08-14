from turingdb import TuringDB

client = TuringDB(host="http://localhost:6666")
graph = "test_graph4"
client.query(f"CREATE GRAPH {graph}")
client.set_graph(graph)

for i in range(11):
    change = client.query("CHANGE NEW")["Change ID"][0]
    client.checkout(change=int(change))
    print(f"Submitting change {change}")
    client.query("CHANGE SUBMIT")
    client.checkout()
