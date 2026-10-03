// WITH [0, 1, 6, 99] AS ss UNWIND range(0, 3) AS i MATCH (m)-[e]->(n:Person) WHERE n = ss[i] RETURN m.name, n.name
func.func @main() {
  %0 = db.constant([0, 1, 6, 99])
  %1 = db.constant(0)
  %2 = db.constant(3)
  %3 = db.range(%1, %2)
  %element = db.unwind(%3, {})
  %4 = db.list_index %0, %element
  %nodes = db.fetch_nodes(%4, {})
  %5 = db.get_node_label_set(%nodes)
  %6 = db.check_label_constraint(%5, ["Person"])
  %7 = db.filter(%6, {%nodes})
  %8, %9, %10, %11 = db.get_in_edges(%7, {})
  %12 = db.get_node_properties(%8, "name")
  %13 = db.get_node_properties(%11, "name")
  db.output(%12, %13) names ["m.name", "n.name"]
  return
}
