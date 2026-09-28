func.func @main() {
  %0:2 = db.cross_product factor {
    %17 = db.scan_nodes()
    db.yield %17
  } factor {
    %17 = db.scan_nodes()
    db.yield %17
  }
  %1 = db.get_node_properties(%0#0, "name")
  %2 = db.constant("Cyrus")
  %3 = db.eq %1, %2
  %4 = db.get_node_properties(%0#1, "name")
  %5 = db.get_node_properties(%0#0, "name")
  %6 = db.eq %4, %5
  %7 = db.and %3, %6
  %8 = db.get_node_properties(%0#1, "age")
  %9 = db.constant(32)
  %10 = db.eq %8, %9
  %11 = db.and %7, %10
  %12:2 = db.filter(%11, {%0#1, %0#0})
  %13 = db.get_node_properties(%12#1, "isFrench")
  %14 = db.get_node_properties(%12#0, "isFrench")
  %15 = db.eq %13, %14
  %16:2 = db.filter(%15, {%12#0, %12#1})
  db.output(%16#1)
  return
}
