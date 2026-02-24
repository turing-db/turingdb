import turingdb

t = turingdb.TuringDB(host="http://localhost:6666")
t.try_reach()

print("* create_turingdb_object: done")
