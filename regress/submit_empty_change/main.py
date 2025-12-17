from pytester import TuringdbTester

tester = TuringdbTester()
tester.spawn()
t = tester.client()

# Create a change
t.set_graph("default")
change = t.query("CHANGE NEW")["changeID"][0]

# Submit change
t.checkout(change=str(change))
print("Submit change " + str(change))
t.query("CHANGE SUBMIT")

print("* submit_empty_change: done")
