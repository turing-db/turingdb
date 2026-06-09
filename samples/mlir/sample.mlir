module {
  func.func @main() {
    %0 = "db.scan_nodes"() : () -> !db.column<"scan">
    %1:4 = "db.get_out_edges"(%0) : (!db.column<"scan">) -> (!db.column<"srcs">, !db.column<"eids">, !db.column<"etypes">, !db.column<"tgts">)
    return
  }
}
