// MATCH (a:Person) MERGE (a)-[e:INTERESTED_IN]->(b:Interest {name: 'Bio'})

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %1 = db.constant("Bio" : !storage.string)
  %2, %3, %4, %5, %6, %7 = db.merge nodes [[], ["Interest"]] props [[], ["name"]]
                                    edges ["INTERESTED_IN"] props [[]] dirs [forward]
                                    bound {%0} pending [] {} values {%1} {} carrying {%0}
    : (!db.column<!storage.node_id>, !db.column<!storage.string>, !db.column<!storage.node_id>)
      -> (!db.column<!storage.node_id>, !db.column<!storage.bool>,
          !db.column<!storage.edge_id>, !db.column<!storage.bool>,
          !db.column<!storage.bool>, !db.column<!storage.node_id>)
  db.output(%2) : !db.column<!storage.node_id>
  return
}
