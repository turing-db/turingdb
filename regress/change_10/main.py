from pytester import TuringdbTester

tester = TuringdbTester()
tester.spawn()
t = tester.client()

graph = "mygraph"
t.query(f"CREATE GRAPH {graph}")
t.set_graph(graph)

for i in range(11):
    change = t.query("CHANGE NEW")["changeID"][0]
    t.checkout(change=change)
    print(f"Submitting change {change}")
    t.query("CHANGE SUBMIT")
    t.checkout()
