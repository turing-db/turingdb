module {
  func.func @second() {
    %0 = db.scan_nodes() : !db.column<none>
    return
  }
}
