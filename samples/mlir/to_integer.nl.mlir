// RETURN toInteger("42")

module {
  func.func @main() {
    %0 = nl.constant("42" : !storage.string)
    %1 = nl.to_integer %0 : (!nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i64>>
    nl.output(%1) : !nl.chunk<!storage.nullable<i64>>
    return
  }
}
