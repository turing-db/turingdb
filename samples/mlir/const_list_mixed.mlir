module {
  func.func @main() {
    // RETURN [1, "Hello", [1]]: a list literal whose elements share no type. The sibling of
    // const_list.mlir, which holds three integers - this one shows what the other's comment
    // describes, the type-erased verdict.
    //
    // db.constant infers the column from the elements, and there is no one type to infer:
    // the integer and the string disagree, and the nested list carries none at all (it rides
    // an array attribute, which is untyped). So the verdict is a list of type-erased tagged
    // scalars - !storage.list<!storage.list_element> - where const_list.mlir's agreeing
    // elements give !storage.list<i64>.
    //
    // Each cell keeps its own type tag at runtime, so the string stays a string and the
    // nested list stays one list held as a single element of the outer one. A null element
    // and an empty list reach the same erased verdict by the same rule.
    //
    // Needs no -graph: nothing here is resolved against a schema.
    %xs = db.constant([1, "Hello", [1]])

    db.output(%xs) : !db.column<!storage.list<!storage.list_element>>

    return
  }
}
