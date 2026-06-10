// The nested-loop form of: db.scan_nodes -> db.get_out_edges.
// Each nl.for step pulls the next chunks from a database iterator.
module {
    func.func @nested_loop() {
        %nodes = nl.scan_nodes() : !nl.iter<!nl.chunk<!nl.node_id>>

        nl.for %chunk in %nodes : !nl.iter<!nl.chunk<!nl.node_id>> {
            %edges = nl.get_out_edges(%chunk) : (!nl.chunk<!nl.node_id>) -> !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>>

            nl.for %srcs, %eids, %etypes, %tgts in %edges : !nl.iter<!nl.chunk<!nl.node_id>, !nl.chunk<!nl.edge_id>, !nl.chunk<!nl.edge_type_id>, !nl.chunk<!nl.node_id>> {
                nl.yield
            }

            nl.yield
        }

        return
    }
}
