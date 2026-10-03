// UNWIND [0, 6, 99] AS x MATCH (n)-[e:INTERESTED_IN]->(m) WHERE n = x RETURN n.name, m.name
func.func @main() {
  %0 = db.const_scan_nodes([0, 6, 99])
  %1, %2, %3, %4 = db.get_out_edges_by_type(%0, ["INTERESTED_IN"], {})
  %5 = db.get_node_properties(%1, "name")
  %6 = db.get_node_properties(%4, "name")
  db.output(%5, %6) names ["n.name", "m.name"]
  return
}
