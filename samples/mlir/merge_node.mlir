// MERGE (n:Person {name: 'Alice'})

func.func @main() {
  %0 = db.constant("Alice" : !storage.string)
  %1, %2, %3 = db.merge nodes [["Person"]] props [["name"]]
                        edges [] props [] dirs []
                        bound {} pending [] {} values {%0} {} carrying {}
    : (!db.column<!storage.string>)
      -> (!db.column<!storage.node_id>, !db.column<!storage.bool>, !db.column<!storage.bool>)
  return
}
