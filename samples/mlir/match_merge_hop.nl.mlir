// MATCH (a:Person) MERGE (a)-[e:INTERESTED_IN]->(b:Interest {name: 'Bio'})

func.func @main() {
  %0 = nl.scan_nodes_by_label(["Person"])
  %1 = nl.constant("Bio")
  nl.for %arg0 in %0 {
    %2, %3, %4, %5, %6, %7 = nl.merge nodes [[], ["Interest"]] props [[], ["name"]]
                                      edges ["INTERESTED_IN"] props [[]] dirs [forward]
                                      bound {%arg0} pending [] {} values {%1} {} carrying {%arg0}
    nl.output(%2)
  }
  return
}
