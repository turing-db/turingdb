// MERGE (n:Person {name: 'Alice'})

func.func @main() {
  %0 = nl.constant("Alice")
  %1, %2, %3 = nl.merge nodes [["Person"]] props [["name"]]
                        edges [] props [] dirs []
                        bound {} pending [] {} values {%0} {} carrying {}
  return
}
