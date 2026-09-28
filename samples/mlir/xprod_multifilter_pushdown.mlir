// MATCH (n), (m) WHERE n.age = 32 and n.name = 'Remy' AND m.age IS NULL RETURN *

func.func @main() {
  %0:2 = db.cross_product factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
    %3 = db.constant(32 : i64)
    %4 = db.eq %2, %3 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
    %5 = db.filter(%4, {%1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    %6 = db.get_node_properties(%5, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
    %7 = db.constant("Remy" : !storage.string)
    %8 = db.eq %6, %7 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
    %9 = db.filter(%8, {%5}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.yield %9 : !db.column<!storage.node_id>
  } factor {
    %1 = db.scan_nodes() : !db.column<!storage.node_id>
    %2 = db.get_node_properties(%1, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
    %3 = db.constant("" : !storage.nullable<none>)
    %4 = db.eq %2, %3 : (!db.column<none>, !db.column<!storage.nullable<none>>) -> !db.column<!storage.bool>
    %5 = db.filter(%4, {%1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.yield %5 : !db.column<!storage.node_id>
  }
  db.output(%0#0, %0#1) names ["n", "m"] : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
