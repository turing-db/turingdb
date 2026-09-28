// MATCH (n:Person{name:'Cyrus'}) WHERE n.age = 23 RETURN n
func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.constant("Cyrus" : !storage.string)
  %3 = db.eq %1, %2 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %4 = db.filter(%3, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %5 = db.get_node_label_set(%4) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %6 = db.check_label_constraint(%5, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %7 = db.filter(%6, {%4}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %8 = db.get_node_properties(%7, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %9 = db.constant(23 : i64)
  %10 = db.eq %8, %9 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %11 = db.filter(%10, {%7}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%11) : !db.column<!storage.node_id>
  return
}
