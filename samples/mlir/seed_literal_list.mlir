// UNWIND [0, 6, 99] AS x MATCH (m)-[e]->(n:Person) WHERE n = x RETURN m.name, n.name
func.func @main() {
  %0 = db.unwind_const([0, 6, 99])
  %nodes = db.fetch_nodes(%0, {})
  %1 = db.get_node_label_set(%nodes)
  %2 = db.check_label_constraint(%1, ["Person"])
  %3 = db.filter(%2, {%nodes})
  %4, %5, %6, %7 = db.get_in_edges(%3, {})
  %8 = db.get_node_properties(%4, "name")
  %9 = db.get_node_properties(%7, "name")
  db.output(%8, %9) names ["m.name", "n.name"]
  return
}
