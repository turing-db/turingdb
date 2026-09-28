func.func @main() {
  %0:2 = db.cross_product factor {
    %a = db.scan_nodes()
    db.yield %a
  } factor {
    %b = db.scan_nodes()
    db.yield %b
  }
  %c = db.constant(5)
  db.output(%c)
  return
}
