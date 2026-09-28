// MATCH (n)-->()-->() WHERE n.age = 32 RETURN *

func.func @main() {
  %0 = db.scan_nodes()
  %1 = db.get_node_properties(%0, "age")
  %2 = db.constant(32)
  %3 = db.eq %1, %2
  %4 = db.filter(%3, {%0})
  %5, %6, %7, %8 = db.get_out_edges(%4, {})
  %9, %10, %11, %12, %13, %14, %15 = db.get_out_edges(%8, {%5, %6, %7})
  db.output(%13) names ["n"]
  return
}
