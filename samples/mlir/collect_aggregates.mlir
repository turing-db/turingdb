module {
  func.func @main() {
    // MATCH (a) WITH a.dob AS dob, collect(a.name) AS names, count(a.age) AS n,
    // min(a.age) AS low, max(a.age) AS high RETURN dob, names, n, low, high: one
    // accumulator holds a group's collected list and the reductions over the same
    // rows, so the aggregates ride the collect rather than a second op grouping those
    // rows again.
    //
    // The operands are the keyCount grouping keys, then the one collected column, then
    // one input per kind the `aggregates` list names - the same inputs, in the same
    // order, that db.group_aggregate takes. The results mirror them: the keys
    // unchanged, the list, then one column per kind.
    //
    // Nothing keeps a reduction off the collected column, and nothing makes the three
    // read different columns: all three here reduce the same "age". Their result types
    // are resolved during lowering, as db.group_aggregate's are - count is a non-null
    // ui64 per group, min and max keep the reduced column's type, so they are left
    // !db.column<none> here.
    //
    // "dob", "name" and "age" are simpledb properties; swap them for properties your
    // graph has.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %dob = db.get_node_properties(%a, "dob") : (!db.column<!storage.node_id>) -> !db.column<none>

    %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>

    %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<none>

    %gdob, %names, %n, %low, %high = db.collect(%dob, %name, %age, %age, %age) keys 1 aggregates [count, min, max] : (!db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>, !db.column<ui64>, !db.column<none>, !db.column<none>)

    db.output(%gdob, %names, %n, %low, %high) : !db.column<none>, !db.column<!storage.list<none>>, !db.column<ui64>, !db.column<none>, !db.column<none>

    return
  }
}
