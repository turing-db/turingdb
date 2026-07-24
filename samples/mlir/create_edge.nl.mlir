// CREATE (n:Person)-[e:LIKES]->(i:Interest)
func.func @main() {
  %0 = nl.create_node ["Person"], [], {}
  %1 = nl.create_node ["Interest"], [], {}
  %2 = nl.create_edge %0, %1, "LIKES", [], {}
  return
}

