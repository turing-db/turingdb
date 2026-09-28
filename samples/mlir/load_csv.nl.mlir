// LOAD CSV 'people.csv' WITH HEADERS AS row RETURN row.name, toInteger(row.age)

// Generated nl-dialect lowering of load_csv.mlir (db dialect).
// Reproduce with: mlir -f load_csv.mlir -l
// This is the DBLowering output; edit load_csv.mlir, not this file.
//
// Needs no -graph: the chunk element types come from the load, not from a schema.
// db.load_csv lowers to an nl.load_csv source op whose nl.for binds one chunk per field,
// each step parsing the next chunk of records into them.
//
// The path and the field list are forwarded as-is: the path is resolved against the
// session's data directory, and a header name against the file's header line, when the
// loop runs. Every field rides an owning string chunk, which is what a conversion reads
// and what a write stores a String property from.
module {
  func.func @main() {
    %0 = nl.load_csv("people.csv", ["name", "age"]) with_headers : !nl.iter<!nl.chunk<!storage.owned_string>, !nl.chunk<!storage.owned_string>>
    nl.for %arg0, %arg1 in %0 : !nl.iter<!nl.chunk<!storage.owned_string>, !nl.chunk<!storage.owned_string>> {
      %1 = nl.to_integer %arg1 : (!nl.chunk<!storage.owned_string>) -> !nl.chunk<!storage.nullable<i64>>
      nl.output(%arg0, %1) names ["row.name", "toInteger(row.age)"] : !nl.chunk<!storage.owned_string>, !nl.chunk<!storage.nullable<i64>>
    }
    return
  }
}
