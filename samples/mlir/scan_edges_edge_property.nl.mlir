// Generated nl-dialect lowering of scan_edges_edge_property.mlir (db dialect).
// Reproduce with: mlir -dump-lowered scan_edges_edge_property.mlir -graph <graph>
// This is the DBLowering output; edit scan_edges_edge_property.mlir, not this file.
//
// The edge-property sibling of scan_edges.nl.mlir: same nl.scan_edges + two
// nl.get_out_edges hops, but the property fetch is nl.get_edge_properties on the
// last hop's edge chunk (%arg9) rather than nl.get_node_properties on the node.
// Needs -graph: the value chunk element type is resolved from the "duration"
// property during lowering (an Int64 in simpledb), so the fetch produces a
// !storage.nullable<i64> value chunk in the innermost loop, feeding one nl.output.
module {
  func.func @main() {
    %0 = nl.get_property_type("duration")

    // Edge scan: %arg0 is the source, %arg1 the edge, %arg2 the edge type,
    // %arg3 the target `t` the first hop leaves from.
    %1 = nl.scan_edges()
    nl.for %arg0, %arg1, %arg2, %arg3 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      // First out-hop t->b, empty carry set. %arg4 is `t`, %arg7 is `b`.
      %2 = nl.get_out_edges(%arg3, {})
      nl.for %arg4, %arg5, %arg6, %arg7 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        // Second out-hop b->c, empty carry set. %arg9 is the b->c edge `r`,
        // %arg11 is `c`.
        %3 = nl.get_out_edges(%arg7, {})
        nl.for %arg8, %arg9, %arg10, %arg11 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
          // Read r.duration into a nullable i64 chunk and emit (r, r.duration).
          %4 = nl.get_edge_properties(%arg9, %0) : !nl.chunk<!storage.nullable<i64>>
          nl.output(%arg9, %4) : !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.nullable<i64>>
        }
      }
    }
    return
  }
}
