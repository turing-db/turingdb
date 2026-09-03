// LOAD CSV 'people.csv' AS row MATCH (n {name: row[0]}) RETURN n.name, row[2]

// Generated nl-dialect lowering of load_csv_match.mlir (db dialect).
// Reproduce with: mlir -f load_csv_match.mlir -l -g <graph>
// This is the DBLowering output; edit load_csv_match.mlir, not this file.
//
// Needs -graph: the property fetch resolves 'name' against the schema.
//
// The product becomes a nest: the scan's loop outside, the load's inside it, and the
// nl.cross_product in the inner body pairing the outer chunk with the inner one. The
// load's parser is local to each entry of its loop, so the file is reopened and re-read
// once per chunk of the scan - the inner factor of a product restarts on every outer
// step, the same way a nested nl.scan_nodes does.
module {
  func.func @main() {
    %0 = nl.get_property_type("name")
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %2 = nl.load_csv("people.csv", [0 : ui64, 2 : ui64]) : !nl.iter<!nl.chunk<!storage.owned_string>, !nl.chunk<!storage.owned_string>>
      nl.for %arg1, %arg2 in %2 : !nl.iter<!nl.chunk<!storage.owned_string>, !nl.chunk<!storage.owned_string>> {
        %3:3 = nl.cross_product{%arg0} {%arg1, %arg2} : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.owned_string>, !nl.chunk<!storage.owned_string>}
        %4 = nl.get_node_properties(%3#0, %0) : !nl.chunk<!storage.nullable<!storage.string>>
        %5 = nl.eq %4, %3#1 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.owned_string>) -> !nl.chunk<!storage.nullable<i1>>
        %6:3 = nl.filter %5, (%3#0, %3#1, %3#2) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.owned_string>, !nl.chunk<!storage.owned_string>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.owned_string>, !nl.chunk<!storage.owned_string>)
        %7 = nl.get_node_properties(%6#0, %0) : !nl.chunk<!storage.nullable<!storage.string>>
        nl.output(%7, %6#2) names ["n.name", "row[2]"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.owned_string>
      }
    }
    return
  }
}
