// Generated nl-dialect lowering of scan_edges.mlir (db dialect).
// Reproduce with: mlir -dump-lowered scan_edges.mlir -graph <graph>
// This is the DBLowering output; edit scan_edges.mlir, not this file.
//
// Needs -graph: the value chunk element type is resolved from the "name"
// property during lowering (a String in simpledb). db.scan_edges lowers to an
// nl.scan_edges source op whose nl.for binds the four edge chunks - source,
// edge, edge type, target. The two out-hops nest inside, each an
// nl.get_out_edges + nl.for off the previous step's target, and the property
// fetch runs in the innermost body against the final node `c`, feeding one
// nl.output.
module {
  func.func @main() {
    %0 = nl.get_property_type("name")

    // Edge scan: %arg0 is the source, %arg1 the edge, %arg2 the edge type,
    // %arg3 the target `t` the first hop leaves from.
    %1 = nl.scan_edges()
    nl.for %arg0, %arg1, %arg2, %arg3 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      // First out-hop t->b, empty carry set. %arg4 is `t`, %arg7 is `b`.
      %2 = nl.get_out_edges(%arg3, {})
      nl.for %arg4, %arg5, %arg6, %arg7 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        // Second out-hop b->c, empty carry set. %arg8 is `b`, %arg11 is `c`.
        %3 = nl.get_out_edges(%arg7, {})
        nl.for %arg8, %arg9, %arg10, %arg11 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
          // Read c.name into a nullable string chunk and emit (c, c.name).
          %4 = nl.get_node_properties(%arg11, %0) : !nl.chunk<!storage.nullable<!storage.string>>
          nl.output(%arg11, %4) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>
        }
      }
    }
    return
  }
}
