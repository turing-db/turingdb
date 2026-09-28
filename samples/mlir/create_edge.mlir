// CREATE (n:Person)-[e:LIKES]->(i:Interest)
func.func @main() {
  %0 = db.create_node(["Person"], [], {})
  %1 = db.create_node(["Interest"], [], {})
  %2 = db.create_edge(%0, %1, "LIKES", [], {})
  return
}
