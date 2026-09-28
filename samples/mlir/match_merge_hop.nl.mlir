// MATCH (a:Person) MERGE (a)-[e:INTERESTED_IN]->(b:Interest {name: 'Bio'})

func.func @main() {
  %0 = nl.scan_nodes_by_label(["Person"])
  %1 = nl.constant("Bio" : !storage.string)
  nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %2, %3, %4, %5, %6, %7 = nl.merge nodes [[], ["Interest"]] props [[], ["name"]]
                                      edges ["INTERESTED_IN"] props [[]] dirs [forward]
                                      bound {%arg0} pending [] {} values {%1} {} carrying {%arg0}
      : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.string>, !nl.chunk<!storage.node_id>)
        -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.bool>,
            !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.bool>,
            !nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>)
    nl.output(%2) : !nl.chunk<!storage.node_id>
  }
  return
}
