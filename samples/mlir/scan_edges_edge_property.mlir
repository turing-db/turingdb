module {
  func.func @main() {
    // The edge-property sibling of scan_edges.mlir: same scan + two out-hops,
    // but the property read is on the last hop's EDGE, not the final node:
    //
    //   MATCH ()-[]->(t)-->(b)-[r]->(c) RETURN r, r.duration
    //
    // db.scan_edges opens the dataflow with the whole edge set, the two
    // get_out_edges hops chain off the target column, and the second hop's edge
    // column %h2e feeds db.get_edge_properties - the edge counterpart of
    // get_node_properties, reading from the edge side of the graph.
    %src, %e, %et, %t = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    // First out-hop t->b, empty carry set. %h1src is `t`, %b its successor.
    %h1src, %h1e, %h1et, %b = db.get_out_edges(%t, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    // Second out-hop b->c, empty carry set. %h2e is the b->c edge `r`, %c the node.
    %h2src, %h2e, %h2et, %c = db.get_out_edges(%b, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    // Read the "duration" property of the last hop's edge `r`. An edge that lacks
    // it comes back null - none are dropped. "duration" must exist in the loaded
    // graph (-graph): the db -> nl lowering resolves the name against the schema
    // and bakes its value type into the result chunk (in simpledb "duration" is
    // an Int64). The input is an edge ID column, so the op chosen is the edge-side
    // db.get_edge_properties.
    %dur = db.get_edge_properties(%h2e, "duration") : (!db.column<!storage.edge_id>) -> !db.column<none>

    db.output(%h2e, %dur) : !db.column<!storage.edge_id>, !db.column<none>

    return
  }
}
