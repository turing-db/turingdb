// MATCH (n) WHERE n.age = 32 RETURN n, as codegen emits it before the passes run

func.func @main() {
  %0 = db.scan_nodes()
  %1 = db.get_node_properties(%0, "age")
  %2 = db.constant(32)
  %3 = db.eq %1, %2
  %4 = db.filter(%3, {%0})
  db.output(%4) names ["n"]
  return
}
