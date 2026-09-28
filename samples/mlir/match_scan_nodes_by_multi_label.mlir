// MATCH (n:Person:SoftwareEngineering) RETURN  *

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person", "SoftwareEngineering"])
  db.output(%0) names ["n"]
  return
}
