module {
  func.func @main() {
    // RETURN [1, 2, 3]: a list literal as a value. It is known at plan time and reads no
    // row, so - unlike db.unwind_const, which spreads the same literals over one row per
    // element - it holds the whole list as one cell standing for every row. That is what
    // db.constant already means, so a list is one of its values rather than an op of its
    // own: the literals ride it as an array of per-element attributes.
    //
    // An array attribute carries no type, so db.constant infers the column from the
    // elements - the homogeneity verdict. These three share one type, so it is a list of
    // i64. Elements that disagree, or one that is itself a list or a null, carry no shared
    // type and give a list of type-erased tagged scalars instead:
    //
    //   %xs = db.constant([10, true, [1, 2]])   // !db.column<!storage.list<!storage.list_element>>
    //
    // An empty list is that same type-erased form and is still a value: one list holding no
    // element, where UNWIND of it yields no row at all. The result type is inferred and so
    // never printed, exactly as for a scalar db.constant.
    //
    // Needs no -graph: nothing here is resolved against a schema.
    %xs = db.constant([1, 2, 3])

    db.output(%xs) : !db.column<!storage.list<i64>>

    return
  }
}
