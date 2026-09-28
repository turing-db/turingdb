// MATCH (n), (m) WHERE n.age = 32 and n.name = 'Remy' AND m.age IS NULL RETURN *

func.func @main() {
  %0:2 = db.cross_product factor {
    %1 = db.scan_nodes()
    %2 = db.get_node_properties(%1, "age")
    %3 = db.constant(32)
    %4 = db.eq %2, %3
    %5 = db.filter(%4, {%1})
    %6 = db.get_node_properties(%5, "name")
    %7 = db.constant("Remy")
    %8 = db.eq %6, %7
    %9 = db.filter(%8, {%5})
    db.yield %9
  } factor {
    %1 = db.scan_nodes()
    %2 = db.get_node_properties(%1, "age")
    %3 = db.constant("")
    %4 = db.eq %2, %3
    %5 = db.filter(%4, {%1})
    db.yield %5
  }
  db.output(%0#0, %0#1) names ["n", "m"]
  return
}
