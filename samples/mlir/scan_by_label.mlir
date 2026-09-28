module {
  func.func @main() {
    // MATCH (a:Person) RETURN a: scan only the nodes carrying the Person label
    // rather than every node. The label list is ANDed - MATCH (a:Person:Employee)
    // is db.scan_nodes_by_label(["Person", "Employee"]) - and matched as a
    // superset, so a node labelled Person:Employee:Manager still matches
    // ["Person", "Employee"].
    //
    // The labels are names spelled the same as in the query; execution resolves
    // each against the loaded graph's schema (-graph). A label no node was ever
    // created with makes the conjunction unsatisfiable, so the scan yields no
    // rows. Swap "Person" for a label your graph actually has.
    %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>

    db.output(%a) : !db.column<!storage.node_id>

    return
  }
}
