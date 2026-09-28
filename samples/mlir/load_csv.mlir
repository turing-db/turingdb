// LOAD CSV 'people.csv' WITH HEADERS AS row RETURN row.name, toInteger(row.age)

module {
  func.func @main() {
    // The CSV source. The file is fixed by the query and reads no row, so it opens a
    // dataflow one row per record, the way db.unwind_const opens one from a literal list.
    //
    // A record is many values and an op has a fixed number of results, so the row is not
    // one column: the field list names the fields the query reads and the op produces one
    // column per name, in that order. A string entry is a header - only a with_headers
    // load resolves one - and a ui64 entry is the position row[2] names. A field the
    // query never names is not produced at all, so the 768 columns of an embedding file
    // cost nothing to skip, and a field named twice is produced once.
    //
    // Every field is a string, since a CSV file carries no types: the query converts them
    // itself, which is the db.to_integer below. The characters live in the column that
    // parsed them rather than in the graph, so the columns own their strings -
    // !storage.owned_string, not the !storage.string a property fetch borrows.
    //
    // Needs no -graph: nothing here is resolved against a schema. -exec cannot run it
    // either, though: the path is resolved against the session's data directory, which
    // this tool's interpreter opens none of.
    //
    // With incoming rows (LOAD CSV ... MATCH (a)) the source is crossed with them through
    // db.cross_product - see load_csv_match.mlir; there is no expanding variant of this
    // op.
    //
    // The positional form, and ON ERROR SKIP, which drops a malformed record instead of
    // failing the query:
    //   %r:2 = db.load_csv("people.csv", [0 : ui64, 2 : ui64]) skip_on_error
    //     : !db.column<!storage.owned_string>, !db.column<!storage.owned_string>
    %0:2 = db.load_csv("people.csv", ["name", "age"]) with_headers : !db.column<!storage.owned_string>, !db.column<!storage.owned_string>

    %1 = db.to_integer(%0#1) : (!db.column<!storage.owned_string>) -> !db.column<none>

    db.output(%0#0, %1) names ["row.name", "toInteger(row.age)"] : !db.column<!storage.owned_string>, !db.column<none>

    return
  }
}
