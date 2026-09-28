// MATCH (n:Person:SoftwareEngineering)-->() RETURN  *

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person", "SoftwareEngineering"])
  %1, %2, %3, %4 = db.get_out_edges(%0, {})
  db.output(%1) names ["n"]
  return
}
