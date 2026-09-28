module {
  func.func @main() {
    // MATCH (a) WITH a.age AS age, collect(a.name) AS names UNWIND names AS name
    // RETURN age, name: the fused collect-then-unwind. Group the nodes by age and
    // collect each group's names, then immediately expand each list back into one row
    // per name - so the result is one (age, name) row per node, regrouped by age. The
    // list never materializes: the accumulator's per-group values are re-emitted
    // element by element.
    //
    // keys 1 marks the leading column (age) as the grouping key; the single trailing
    // column (name) is collected then unwound, so the op emits one row per collected
    // element: the group's age (repeated across its elements) then one name. An empty
    // or all-null group contributes no row, matching UNWIND of an empty list. The
    // unwound value type is resolved during lowering (hence -graph); name is a String.
    //
    // db.unwind_collect is a pipeline breaker: it lowers to a hoisted nl.collect_buffer,
    // an nl.collect_update inside the scan loop, and - after the loop - an nl.unwind_collect
    // source iterator whose nl.for emits one row per collected element.
    //
    // "age" and "name" are simpledb properties; swap them for properties your graph has.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<none>

    %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>

    %gage, %uname = db.unwind_collect(%age, %name) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>)

    db.output(%gage, %uname) : !db.column<none>, !db.column<none>

    return
  }
}
