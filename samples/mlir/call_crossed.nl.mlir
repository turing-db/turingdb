// MATCH (n:Person) CALL db.labels() YIELD label RETURN n.name, label

func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.procedure("db.labels") yields ["label"]
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.procedure_init(%1, (), {}) : (!nl.procedure_state) -> !nl.iter<!nl.chunk<!storage.string>>
    nl.for %arg1 in %3 : !nl.iter<!nl.chunk<!storage.string>> {
      %4:2 = nl.cross_product{%arg0} {%arg1} : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.string>}
      %5 = nl.get_node_properties(%4#0, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%5, %4#1) names ["n.name", "label"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>
    }
  }
  return
}
