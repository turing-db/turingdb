import turingdb

t = turingdb.TuringDB(instance_id="", auth_token="", host="http://localhost:6666")

# Create a graph
res = t.query("CREATE GRAPH mygraph")
print(res)

# Create a change
t.set_graph("mygraph")
change = t.query("CHANGE NEW")["changeID"][0]

t.checkout(change=str(change))
t.query("CREATE (:Person {name: 'Alice'})")
t.query("CREATE (:Person {name: 'Bob'})")
t.query("CREATE (:Person {name: 'Charlie'})")
t.query("CHANGE SUBMIT")
t.checkout("main")

change = t.query("CHANGE NEW")["changeID"][0]
t.checkout(change=str(change))
t.query("CREATE (:Person :Employee {name: 'Dave'})")
t.query("CREATE (:Person :Employee {name: 'Eve', age: 19})")
t.query("CREATE (:Person :Employee {name: 'Mallory', age: 31})")
t.query("CREATE (:Person :Employee {name: 'Trent', age: 32})")
t.query(
    "CREATE (:Person :Employee {name: 'Wendy'})-[e:WORKS_FOR]->(:Company {name: 'Turing'})"
)
t.query(
    "CREATE (:Company { name: 'Turing' })-[e:WORKS_WITH]->(:Company { name: 'Some company' })"
)

t.query("COMMIT")
t.query("CHANGE SUBMIT")

# Go on to main
t.checkout("main")
df = t.query("CALL db.labels()")
print(df)
assert df.shape == (3, 2)
assert df["id"].tolist() == [0, 1, 2]
assert df["label"].tolist() == ["Person", "Employee", "Company"]

df = t.query("CALL db.edgeTypes()")
print(df)
assert df.shape == (2, 2)
assert df["id"].tolist() == [0, 1]
assert df["edgeType"].tolist() == ["WORKS_FOR", "WORKS_WITH"]

df = t.query("CALL db.propertyTypes()")
print(df)
assert df.shape == (2, 3)
assert df["id"].tolist() == [0, 1]
assert df["propertyType"].tolist() == ["name", "age"]
assert df["valueType"].tolist() == ["String", "Int64"]

df = t.query("CALL db.history()")
print(df)
assert df.shape == (4, 4)
