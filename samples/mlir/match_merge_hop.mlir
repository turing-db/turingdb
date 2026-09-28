// MATCH (a:Person) MERGE (a)-[e:INTERESTED_IN]->(b:Interest {name: 'Bio'})

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"])
  %1 = db.constant("Bio")
  %2, %3, %4, %5, %6, %7 = db.merge nodes [[], ["Interest"]] props [[], ["name"]]
                                    edges ["INTERESTED_IN"] props [[]] dirs [forward]
                                    bound {%0} pending [] {} values {%1} {} carrying {%0}
  db.output(%2)
  return
}
