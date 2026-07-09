module {
  func.func @main() {
    // RETURN 10 + 20: two constants added row-wise. db.add takes two value columns
    // and produces one; here both operands are constants, so the result is a single
    // broadcast value. The operand and result element types are spelled in the IR -
    // the db dialect keeps values loosely typed - and the numeric promotion (a float
    // operand would make the result a float) is settled during lowering.
    %x = db.constant(10 : i64)
    %y = db.constant(20 : i64)
    %s = db.add %x, %y : (!db.column<i64>, !db.column<i64>) -> !db.column<i64>

    db.output(%s) : !db.column<i64>

    return
  }
}
