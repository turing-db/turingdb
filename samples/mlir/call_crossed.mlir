// MATCH (n:Person) CALL db.labels() YIELD label RETURN n.name, label

func.func @main() {
  %0:2 = db.cross_product factor {
    %2 = db.scan_nodes_by_label(["Person"])
    db.yield %2
  } factor {
    %2 = db.call_procedure("db.labels", {}, {}) yields ["label"]
    db.yield %2
  }
  %1 = db.get_node_properties(%0#0, "name")
  db.output(%1, %0#1) names ["n.name", "label"]
  return
}
