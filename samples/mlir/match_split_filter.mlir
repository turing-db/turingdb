// MATCH (n) WHERE n.age = 32 AND n.isFrench = true RETURN *

func.func @main() {
  %0 = db.scan_nodes()
  %1 = db.get_node_properties(%0, "age")
  %2 = db.constant(32)
  %3 = db.eq %1, %2
  %4 = db.filter(%3, {%0})
  %5 = db.get_node_properties(%4, "isFrench")
  %6 = db.constant(true)
  %7 = db.eq %5, %6
  %8 = db.filter(%7, {%4})
  db.output(%8) names ["n"]
  return
}
