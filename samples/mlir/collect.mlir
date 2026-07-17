module {
  func.func @main() {
    // MATCH (a) WITH a.age AS age, collect(a.name) AS names RETURN age, names: scan
    // every node, read its "age" and "name", group the nodes by age and collect each
    // group's names into a list. keys 1 marks the leading column (age) as the grouping
    // key; the single trailing column (name) is collected, so the op takes one key
    // column and one value column and emits one row per age: the age, then the list of
    // that group's names.
    //
    // Cypher collect drops nulls within a group, so a node with no "name" contributes
    // nothing to its group's list. The collected element type is resolved during the
    // db -> nl lowering (hence -graph), so the list result is left !storage.list<none>
    // here - name is a String, so it becomes !storage.list<!storage.string>.
    //
    // db.collect is a pipeline breaker like db.group_aggregate: it lowers to a hoisted
    // nl.collect_buffer accumulator, an nl.collect_update inside the scan loop that
    // appends each chunk's values to the per-group lists, and - after the loop - an
    // nl.collect source iterator whose nl.for emits one row per group, each carrying a
    // ListView over that group's elements.
    //
    // "age" and "name" are simpledb properties; swap them for properties your graph has.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %age = db.get_node_properties(%a, "age") : (!db.column<!storage.node_id>) -> !db.column<none>

    %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>

    %gage, %names = db.collect(%age, %name) keys 1 : (!db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<!storage.list<none>>)

    db.output(%gage, %names) : !db.column<none>, !db.column<!storage.list<none>>

    return
  }
}
