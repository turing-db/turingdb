module {
  func.func @main() {
    // MATCH (a), (b) RETURN a, b LIMIT 5: a cross product capped to five pairs.
    //
    // The two factors each scan all nodes; the cross product pairs every `a`
    // with every `b`. With the budget, the nl.cross_product builds only the
    // five-row prefix it can emit (rather than the full N*M product), the
    // nl.limit_update charges that post-cross-product row count, and the
    // nl.limit_truncate cuts it to five before nl.output.
    %0:2 = db.cross_product factor {
      %a = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %a : !db.column<!storage.node_id>
    } factor {
      %b = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %b : !db.column<!storage.node_id>
    }

    %la, %lb = db.limit(%0#0, %0#1) count 5 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)

    db.output(%la, %lb) : !db.column<!storage.node_id>, !db.column<!storage.node_id>

    return
  }
}
