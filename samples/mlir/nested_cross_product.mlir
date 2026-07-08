module {
  func.func @main() {
    // MATCH (a), (b), (c) RETURN a, b, c: a three-way cross product.
    //
    // The db dialect has no n-ary cross_product, so a third factor is expressed
    // by nesting: the outer product's left factor is itself a db.cross_product
    // crossing (a) with (b), and the outer product crosses that (a, b) pair with
    // (c). Lowering nests the inner product's loops inside the outer product's,
    // so the whole thing becomes one deeper loop nest - the inner nl.cross_product
    // materializes the (a, b) pairs once per (a, b) chunk pair, and the outer
    // nl.cross_product crosses that with each `c` chunk. The result is every
    // (a, b, c) triple over the node set, N^3 rows.
    %0:3 = db.cross_product factor {
      %1:2 = db.cross_product factor {
        %a = db.scan_nodes() : !db.column<!storage.node_id>
        db.yield %a : !db.column<!storage.node_id>
      } factor {
        %b = db.scan_nodes() : !db.column<!storage.node_id>
        db.yield %b : !db.column<!storage.node_id>
      }
      db.yield %1#0, %1#1 : !db.column<!storage.node_id>, !db.column<!storage.node_id>
    } factor {
      %c = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %c : !db.column<!storage.node_id>
    }

    db.output(%0#0, %0#1, %0#2) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>

    return
  }
}
