import turingdb

# The server is started with TURING_MAX_DATAPARTS=4 (see run.sh). Each committed
# CREATE adds one data part; MERGE_DATAPARTS compacts them back to a single part.
# COMMIT and CHANGE SUBMIT are rejected once the graph's main-head data parts plus
# the parts already committed by the in-progress change reach the limit.

t = turingdb.TuringDB(host="http://localhost:6666")

t.query("CREATE GRAPH mygraph")
t.set_graph("mygraph")


def submit_one(label):
    change = t.new_change()
    t.checkout(change=change)
    t.query(f"CREATE (:Item {{name: '{label}'}})")
    t.query("COMMIT")
    t.query("CHANGE SUBMIT")
    t.checkout()


def expect_rejected(action, run):
    try:
        run()
    except turingdb.TuringDBException as e:
        assert "MERGE_DATAPARTS" in str(e), f"{action}: unexpected error: {e}"
        print(f"* {action} correctly rejected: {e}")
        return
    raise AssertionError(f"{action}: expected rejection but the query succeeded")


# Fill main up to one below the limit: three committed+submitted parts (limit is 4).
submit_one("a")
submit_one("b")
submit_one("c")

# A change on top of the near-full graph. The first COMMIT still fits (main 3 +
# change 0), but the second reaches the limit (main 3 + change 1) and is rejected,
# as is CHANGE SUBMIT.
change = t.new_change()
t.checkout(change=change)

t.query("CREATE (:Item {name: 'd1'})")
t.query("COMMIT")

t.query("CREATE (:Item {name: 'd2'})")
expect_rejected("COMMIT", lambda: t.query("COMMIT"))
expect_rejected("CHANGE SUBMIT", lambda: t.query("CHANGE SUBMIT"))

# Recovery: discard the blocked change so the graph has no open changes, then
# compact main. Merging frees the data-part budget, so writes resume.
t.query("CHANGE DELETE")
t.checkout()
t.query("MERGE_DATAPARTS")

change = t.new_change()
t.checkout(change=change)
t.query("CREATE (:Item {name: 'e'})")
t.query("COMMIT")
t.query("CHANGE SUBMIT")
t.checkout()

print("* datapart_limit: done")
