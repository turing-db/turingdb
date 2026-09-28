module {
  func.func @main() {
    // UNWIND [1, 2, 3] AS x RETURN x: the literal-list source. The list is known at plan
    // time and reads no row, so it opens a dataflow one row per element, the way
    // db.const_scan_nodes opens one from a fixed set of node IDs.
    //
    // The literals ride the op as an array of typed attributes, each keeping its own
    // type, and the result element type is the homogeneity verdict: these three share
    // one type, so the column is a typed i64 one. A list whose elements disagree - or an
    // empty list - is heterogeneous instead, and unwinds into a type-erased
    // !storage.list_element column of tagged scalars (see the heterogeneous form below).
    // An empty list yields no row.
    //
    // Needs no -graph: nothing here is resolved against a schema.
    //
    // With incoming rows (MATCH (a) UNWIND [1, 2, 3] AS x) the source is crossed with
    // the input through db.cross_product; there is no expanding variant of this op.
    //
    // The heterogeneous form, whose cells need not share a type:
    //   %x = db.unwind_const([true, "mixed", 10]) : !db.column<!storage.list_element>
    %x = db.unwind_const([1, 2, 3]) : !db.column<i64>

    db.output(%x) : !db.column<i64>

    return
  }
}
