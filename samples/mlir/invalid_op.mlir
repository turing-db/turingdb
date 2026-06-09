module {
  func.func @main() {
    %0 = "db.bogus_op"() : () -> !db.column<"x">
    return
  }
}
