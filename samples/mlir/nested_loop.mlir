// The nested-loop form of: db.scan_nodes -> db.get_out_edges / get_in_edges.
// Each nl.for step pulls the next chunks from a database iterator, and
// nl.output sends a chunk on as a column of the query result. The last block
// shows a two-hop MATCH (a)->(b)->(c) using a get_out_edges carry set.
module {
    func.func @main() {
        %nodes = nl.scan_nodes()

        nl.for %chunk in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
            // Forwards: emit each step's target (successor) node IDs
            %out = nl.get_out_edges(%chunk, {})

            nl.for %osrcs, %oeids, %oetypes, %otgts in %out : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
                nl.output(%otgts) : !nl.chunk<!storage.node_id>
                nl.yield
            }

            // Backwards: same chunk shape, emit the source (predecessor) IDs
            %in = nl.get_in_edges(%chunk, {})

            nl.for %isrcs, %ieids, %ietypes, %itgts in %in : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
                nl.output(%isrcs) : !nl.chunk<!storage.node_id>
                nl.yield
            }

            // MATCH (a)->(b)->(c): two out-edge hops, the second carrying `a`.
            // First hop a->b with an empty carry set; %hsrcs is `a`, %hb is `b`.
            %h1 = nl.get_out_edges(%chunk, {})

            nl.for %hsrcs, %he0, %het0, %hb in %h1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
                // Second hop b->c carrying `a` (%hsrcs). Each second-hop edge
                // binds its source(=b) %h2srcs and target(=c) %hc, and the
                // carried `a` comes back filtered as %hafilt - all row-aligned,
                // so we output the (a, b, c) triple.
                %h2 = nl.get_out_edges(%hb, {%hsrcs}) : !nl.chunk<!storage.node_id>

                nl.for %h2srcs, %he1, %het1, %hc, %hafilt in %h2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
                    nl.output(%hafilt, %h2srcs, %hc) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
                    nl.yield
                }

                nl.yield
            }

            nl.yield
        }

        return
    }
}
