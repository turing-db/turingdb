module {
  func.func @main() {
    // Scan every edge, walk two out-hops from each edge's target, then read a
    // property of the final node:
    //
    //   MATCH ()-[]->(t)-->(b)-->(c) RETURN c, c.name
    //
    // db.scan_edges opens the dataflow with the whole edge set - source node,
    // edge, edge type and target node - the same four columns a hop exposes, so
    // the two get_out_edges hops chain straight off the scan's target column.
    // No carry set here: the sample keeps only the final node `c` and its
    // property, so nothing earlier has to ride along.
    %src, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    // First out-hop t->b, empty carry set. %h1src is `t`, %b its successor.
    %h1src, %h1e, %h1et, %b = db.get_out_edges(%t, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    // Second out-hop b->c, empty carry set. %c is the node two hops past `t`.
    %h2src, %h2e, %h2et, %c = db.get_out_edges(%b, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    // Read the "name" property of the final node `c`. A node that lacks it comes
    // back null - none are dropped. "name" must exist in the loaded graph (-graph):
    // the db -> nl lowering resolves the name against the schema and bakes its
    // value type into the result chunk (in simpledb "name" is a String).
    %name = db.get_node_properties(%c, "name") : (!db.column<!storage.node_id>) -> !db.column<none>

    db.output(%c, %name) : !db.column<!storage.node_id>, !db.column<none>

    return
  }
}
