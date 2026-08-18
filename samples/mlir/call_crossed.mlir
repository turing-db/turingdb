// MATCH (n:Person) CALL db.labels() YIELD label RETURN n.name, label

func.func @main() {
  %0:2 = db.cross_product factor {
    %2 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    db.yield %2 : !db.column<!storage.node_id>
  } factor {
    %2 = db.call_procedure("db.labels", {}, {}) yields ["label"] : () -> !db.column<none>
    db.yield %2 : !db.column<none>
  }
  %1 = db.get_node_properties(%0#0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%1, %0#1) names ["n.name", "label"] : !db.column<none>, !db.column<none>
  return
}
