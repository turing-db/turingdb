import turingdb

t = turingdb.TuringClient(host="http://localhost:6666")
t.try_reach()

print("* create_turingdb_object: done")
