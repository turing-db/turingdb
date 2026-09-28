// MATCH (n), (m) WHERE n.name = m.name RETURN n, m

func.func @main() {
  %0:4 = db.hash_join factor {
    %1 = db.scan_nodes()
    %2 = db.get_node_properties(%1, "name")
    db.yield %1, %2
  } factor {
    %1 = db.scan_nodes()
    %2 = db.get_node_properties(%1, "name")
    db.yield %1, %2
  } on 1, 1
  db.output(%0#0, %0#2) names ["n", "m"]
  return
}
