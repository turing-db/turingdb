// MATCH (n:Person:SoftwareEngineering) RETURN  *

func.func @main() {
  %0 = nl.scan_nodes_by_label(["Person", "SoftwareEngineering"])
  nl.for %arg0 in %0 {
    nl.output(%arg0) names ["n"]
  }
  return
}
