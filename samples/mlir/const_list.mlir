module {
  func.func @main() {
    // RETURN [1, 2, 3]: the list literal as a value. The list is known at plan time and
    // reads no row, so - unlike db.unwind_const, which spreads the same literals over one
    // row per element - it holds the whole list as one cell, standing for every row, the
    // way db.constant holds one scalar.
    //
    // The literals ride the op as an array of typed attributes, each keeping its own type,
    // and the list's element type is the homogeneity verdict: these three share one type,
    // so it is a list of i64. Elements that disagree - or one that is itself a list, which
    // carries no type - give a list of type-erased tagged scalars instead:
    //
    //   %xs = db.const_list([10, true, [1, 2]]) : !db.column<!storage.list<!storage.list_element>>
    //
    // An empty list is that same type-erased form and is still a value: one list holding
    // no element, where UNWIND of it yields no row at all.
    //
    // Needs no -graph: nothing here is resolved against a schema.
    %xs = db.const_list([1, 2, 3]) : !db.column<!storage.list<i64>>

    db.output(%xs) : !db.column<!storage.list<i64>>

    return
  }
}
