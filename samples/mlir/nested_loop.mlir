// The nested-loop form of: db.scan_nodes -> db.get_out_edges / get_in_edges.
// Each nl.for step pulls the next chunks from a database iterator, and
// nl.output sends a chunk on as a column of the query result.
module {
    func.func @nested_loop() {
        %nodes = nl.scan_nodes()

        nl.for %chunk in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
            // Forwards: emit each step's target (successor) node IDs
            %out = nl.get_out_edges(%chunk)

            nl.for %osrcs, %oeids, %oetypes, %otgts in %out : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>> {
                nl.output(%otgts) : !nl.chunk<!nl.node_id>
                nl.yield
            }

            // Backwards: same chunk shape, emit the source (predecessor) IDs
            %in = nl.get_in_edges(%chunk)

            nl.for %isrcs, %ieids, %ietypes, %itgts in %in : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>> {
                nl.output(%isrcs) : !nl.chunk<!nl.node_id>
                nl.yield
            }

            nl.yield
        }

        return
    }
}
