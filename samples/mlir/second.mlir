module {
  func.func @second() {
    %0 = db.scan_nodes()
    return
  }
}
