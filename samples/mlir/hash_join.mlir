// MATCH (n), (m) WHERE n.name = m.name RETURN n, m

func.func @main() {
  %0:4 = db.hash_join factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %1, %2 : !db.column<!storage.node_id>, !db.column<none>
  } factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
    db.yield %1, %2 : !db.column<!storage.node_id>, !db.column<none>
  } on 1, 1
  db.output(%0#0, %0#2) names ["n", "m"] : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
