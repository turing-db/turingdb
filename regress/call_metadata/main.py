from pytester import TuringdbTester

tester = TuringdbTester()
tester.spawn()
t = tester.client()

# Create a graph
res = t.query("CREATE GRAPH mygraph")
print(res)
t.set_graph("mygraph")

# Create a change
change = t.query("CHANGE NEW")["changeID"][0]
print(f"Created change {change}")

# Checking out change
res = t.checkout(change=change)

# Make changes
# t.query("CREATE (:Person {name: 'Alice'})")
# t.query("CREATE (:Person {name: 'Bob'})")
# t.query("CREATE (:Person {name: 'Charlie'})")
# t.query("CREATE (:Person, Employee {name: 'Dave'})")
# t.query("CREATE (:Person, Employee {name: 'Eve'})")
# t.query("CREATE (:Person, Employee {name: 'Mallory'})")
# t.query("CREATE (:Person, Employee {name: 'Trent'})")
# t.query("CREATE (:Person, Employee {name: 'Wendy'})") # FIXME properties don't work

t.query("CREATE (:Person)")
t.query("CREATE (:Person)")
t.query("CREATE (:Person)")
t.query("CREATE (:Person:Employee)")
t.query("CREATE (:Person:Employee)")
t.query("CREATE (:Person:Employee)")
t.query("CREATE (:Person:Employee)")
t.query("CREATE (:Person:Employee)")
t.query("CREATE (:Employee)-[e:WORKS_FOR]->(:Company)")

# Submit change
t.query("COMMIT")
t.query("CHANGE SUBMIT")

# Go on to main
t.checkout()

# List labels
print()
print(f" -- Listing labels")
df = t.query("CALL db.labels()")
print(df)
assert df.shape == (3, 2)
assert df["id"][0] == 0
assert df["id"][1] == 1
assert df["id"][2] == 2
assert df["label"][0] == "Person"
assert df["label"][1] == "Employee"
assert df["label"][2] == "Company"

print()
print(f" -- Listing labels with YIELD")
df = t.query("CALL db.labels() YIELD label AS labelName RETURN labelName")
print(df)
assert df.shape == (3, 1)
assert df["labelName"][0] == "Person"
assert df["labelName"][1] == "Employee"
assert df["labelName"][2] == "Company"

# List properties
print()
print(f" -- Listing property types")
df = t.query("CALL db.propertyTypes()")
print(df)
assert df.shape == (0, 3)

# # List relationship types FIXME edge types don't work
# print()
# print(f" -- Listing relationship types")
# df = t.query("CALL db.edgeTypes()")
# print(df)
# assert df.shape == (1, 2)
# assert df["id"][0] == 0
# assert df["edgeType"][0] == "WORKS_FOR"
# 
# print()
# print(f" -- Listing edge types with YIELD")
# df = t.query("CALL db.edgeTypes() YIELD edgeType AS edgeTypeName RETURN edgeTypeName")
# print(df)
# assert df.shape == (1, 2)
# assert df["edgeTypeName"][0] == "WORKS_FOR"

# Checking history
print()
print(f" -- Checking history")
df = t.query("CALL db.history()")
print(df)
assert df.shape == (3, 4)
assert isinstance(df["commit"][0], str)
assert isinstance(df["commit"][1], str)
assert isinstance(df["commit"][2], str)
assert df["nodeCount"][0] == 0
assert df["nodeCount"][1] == 10
assert df["nodeCount"][2] == 0
assert df["edgeCount"][0] == 0
assert df["edgeCount"][1] == 1
assert df["edgeCount"][2] == 0
assert df["partCount"][0] == 0
assert df["partCount"][1] == 1
assert df["partCount"][2] == 0
