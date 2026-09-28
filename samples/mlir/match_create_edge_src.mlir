// MATCH (n) CREATE (n)-[e:COMES_BEFORE]->(m:New)

func.func @main() {
  %0 = db.scan_nodes()
  %1 = db.create_node(["New"], [], {}) foreach %0
  %2 = db.create_edge(%0, %1, "COMES_BEFORE", [], {})
  return
}
