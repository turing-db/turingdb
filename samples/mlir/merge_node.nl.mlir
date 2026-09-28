// MERGE (n:Person {name: 'Alice'})

func.func @main() {
  %0 = nl.constant("Alice" : !storage.string)
  %1, %2, %3 = nl.merge nodes [["Person"]] props [["name"]]
                        edges [] props [] dirs []
                        bound {} pending [] {} values {%0} {} carrying {}
    : (!nl.chunk<!storage.string>)
      -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.bool>, !nl.chunk<!storage.bool>)
  return
}
