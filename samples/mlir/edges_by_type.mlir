module {
  func.func @main() {
    %a = db.scan_nodes()

    %s, %e, %et, %b = db.get_out_edges_by_type(%a, ["KNOWS_WELL"], {})

    db.output(%s, %b)

    return
  }
}
