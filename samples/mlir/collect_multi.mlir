module {
  func.func @main() {
    // MATCH (a) WITH a.dob AS dob, collect(a.name) AS names, collect(DISTINCT a.age) AS
    // ages RETURN dob, names, ages: one accumulator holds every list a group collects,
    // so a second collect is another value column rather than a second op grouping the
    // same rows again - and a second drain the one output could not read.
    //
    // The operands are the keyCount grouping keys, then the collected columns, then one
    // input per kind the `aggregates` list names. What the keys and the aggregates leave
    // over is the value count, so it is not spelled out: two columns follow the one key
    // here. The results mirror them: the keys unchanged, then one list per value.
    //
    // `distinct` names the value columns that dedupe by index rather than being a flag on
    // the op, because one projection may collect a column both ways: here value 1 (the
    // ages) takes each of a group's values once and value 0 (the names) keeps every row.
    //
    // "dob", "name" and "age" are simpledb properties; swap them for properties your
    // graph has.
    %a = db.scan_nodes()

    %dob = db.get_node_properties(%a, "dob")

    %name = db.get_node_properties(%a, "name")

    %age = db.get_node_properties(%a, "age")

    %gdob, %names, %ages = db.collect(%dob, %name, %age) keys 1 distinct [1]

    db.output(%gdob, %names, %ages)

    return
  }
}
