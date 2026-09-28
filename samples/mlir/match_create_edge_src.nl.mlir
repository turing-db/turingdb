// MATCH (n) CREATE (n)-[e:COMES_BEFORE]->(m:New)

func.func @main() {
  %0 = nl.scan_nodes()
  nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %1 = nl.create_node ["New"], [], {} foreach %arg0 : !nl.chunk<!storage.node_id>
    %2 = nl.create_edge %arg0, %1, "COMES_BEFORE", [], {}
  }
  return
}
