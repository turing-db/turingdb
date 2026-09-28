// MATCH (n:Person) RETURN  *

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"])
  db.output(%0) names ["n"]
  return
}
