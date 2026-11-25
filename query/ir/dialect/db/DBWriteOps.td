#ifndef TURINGDB_WRITE_OPS
#define TURINGDB_WRITE_OPS

include "DBDialect.td"
include "DBTypes.td"
include "StorageTypes.td"

def CreateNode : TuringOp<"create_node", [AttrSizedOperandSegments]> {
  let summary = "Create one node per input row, with given labels and property values";

  let description = [{
    `CREATE (n:Person{name:"Alice"})`
      %name = db.constant("Alice" : !db.column<...>)
      %n    = db.create_node (["Person"], ["name"], {%name})
                : (!db.column<none>) -> !db.column<!storage.node_id>

    `MATCH ... CREATE`,
      %a    = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %name = db.get_node_properties(%a, "name") : ... -> !db.column<!storage.nullable<!storage.string>>
      %b    = db.create_node (["Bot"], ["name"], {%name})
                : (!db.column<!storage.nullable<!storage.string>>) -> !db.column<!storage.node_id>
  }];

  let arguments = (ins LabelRefArrayAttr:$labels,
                       StrArrayAttr:$prop_names,
                       Variadic<Column>:$prop_values,
                       Optional<Column>:$cardinality);
  let results   = (outs ColumnNodeIDs:$result);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $labels `,` $prop_names `,` `{` $prop_values `}` `)` ( `foreach` $cardinality^ )?
    attr-dict `:` functional-type(operands, results)
  }];
}

def CreateEdge : TuringOp<"create_edge", [AttrSizedOperandSegments]> {
  let summary = "Create one edge per input row between corresponding source and target nodes";

  let description = [{
    `CREATE (a:Person)-[e:KNOWS]->(b:Person)`
      %n_a = db.create_node (["Person"], [], {}) : () -> !db.column<!storage.node_id>
      %n_b = db.create_node (["Person"], [], {}) : () -> !db.column<!storage.node_id>
      %e   = db.create_edge (%n_a, %n_b, "KNOWS", [], {})
               : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
               -> !db.column<!storage.edge_id>

    `MATCH (a) CREATE (a)-[:KNOWS]->(b:Bot)`
      %a   = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %b   = db.create_node (["Bot"], [], {}) : () -> !db.column<!storage.node_id>
      %e   = db.create_edge (%a, %b, "KNOWS", [], {})
               : (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
               -> !db.column<!storage.edge_id>
  }];

  let arguments = (ins Column:$src_ids,
                       Column:$tgt_ids,
                       EdgeTypeRef:$edge_type,
                       StrArrayAttr:$prop_names,
                       Variadic<Column>:$prop_values,
                       Optional<Column>:$src_pending,
                       Optional<Column>:$tgt_pending,
                       UnitAttr:$src_all_pending,
                       UnitAttr:$tgt_all_pending);
  let results   = (outs ColumnEdgeIDs:$result);
  let hasVerifier = 1;

  // The two optional masks say, row by row, which endpoints are entities this change
  // wrote and has not committed - what a db.merge's node column mixes. A column a
  // db.create_node produced is pending in every row and says so with the matching
  // all_pending flag; one a scan or a traversal bound is pending in none and carries
  // neither a mask nor a flag.
  let assemblyFormat = [{
    `(` $src_ids `,` $tgt_ids `,` $edge_type `,` $prop_names `,` `{` $prop_values `}` `)`
    (`src_pending` $src_pending^)? (`tgt_pending` $tgt_pending^)?
    (`src_all_pending` $src_all_pending^)? (`tgt_all_pending` $tgt_all_pending^)? attr-dict `:`
    functional-type(operands, results)
  }];
}

def Merge : TuringOp<"merge", [AttrSizedOperandSegments]> {
  let summary = "Bind one MERGE path pattern per input row, writing it when the graph does not hold it";

  let description = [{
    The declarative counterpart of Cypher's MERGE. The pattern is one path - a chain
    of nodes joined by hops - and it is matched and written as a unit: for each input
    row the op looks the whole chain up, emits one row per match it finds, and only
    when it finds none writes the entities the pattern does not already have bound
    and emits that one row. That whole-pattern rule is what keeps MERGE from being a
    sequence of per-entity merges: `MERGE (a:P {n: 1})-[:K]->(b:P {n: 2})` writes a
    fresh `a` and `b` when no such hop exists, even when nodes matching each end do.

    The chain is spelled out by the attributes, one entry per entity in pattern order:

      %n, %n_pending, %created = db.merge nodes [["Person"]] props [["name"]]
                                         edges [] props [] dirs []
                                         bound {} pending {} values {%name} {} carrying {}
        : (!db.column<!storage.string>)
          -> (!db.column<!storage.node_id>, !db.column<!storage.bool>, !db.column<!storage.bool>)

    `node_labels` carries each chain node's label set. An empty one marks a node the
    query already bound - `MATCH (a) MERGE (a)-[:K]->(b:Bot)` - whose column comes in
    through `bound_nodes` instead, in chain order; every other node is matched by its
    labels (a superset match, as db.scan_nodes_by_label) and its `node_prop_names`
    values, and written with exactly those when the pattern is missing. A node the
    op writes needs a label, so a node with neither labels nor a bound column is
    rejected.

    `bound_pending` is empty, or holds one boolean column per bound node saying which
    of its rows are entities this change has written but not committed - what an
    earlier db.merge or db.create_node produced. Such a row is in no graph the match
    reads, so the pattern under it can never be found and is written instead.

    `edge_types`, `edge_prop_names` and `edge_directions` carry one entry per hop,
    each hop joining the chain node ahead of it to the one behind. A hop's direction
    is read from the query; `undirected` matches a hop either way round and writes a
    forward one.

    The results pair each entity the op looks up with a boolean mask, nodes in chain
    order then hops: the entity's ID column, then the rows of it that are pending -
    written by this change and not committed, so named by the ID they will commit as,
    which the graph holds nothing under. A bound node has no pair of its own: its rows
    came in through `bound_nodes` and come back through the carry set, re-indexed like
    every other column in flight. After those pairs comes the boolean column saying
    which rows the op wrote the whole pattern for - what ON CREATE and ON MATCH select
    on - then the carry set.
    `carried_columns` mirrors db.get_out_edges': the columns in flight, handed back
    re-indexed onto the rows the merge emitted, so a row the op emitted twice repeats
    them and one it emitted nothing for drops them.
  }];

  let arguments = (ins LabelRefArrayArrayAttr:$node_labels,
                       StrArrayArrayAttr:$node_prop_names,
                       EdgeTypeRefArrayAttr:$edge_types,
                       StrArrayArrayAttr:$edge_prop_names,
                       DenseI64ArrayAttr:$edge_directions,
                       DenseI64ArrayAttr:$pending_nodes,
                       Variadic<Column>:$bound_nodes,
                       Variadic<Column>:$bound_pending,
                       Variadic<Column>:$node_prop_values,
                       Variadic<Column>:$edge_prop_values,
                       Variadic<Column>:$carried_columns);

  let results = (outs Variadic<Column>:$results);

  let hasVerifier = 1;

  // The five attribute lists print ahead of the operands, each under the keyword
  // naming what it describes, so a chain reads left to right. The directions go
  // through the shared custom directive so they print as keywords rather than as
  // the raw integers the DenseI64ArrayAttr stores.
  let assemblyFormat = [{
    `nodes` $node_labels `props` $node_prop_names
    `edges` $edge_types `props` $edge_prop_names `dirs` custom<EdgeDirections>($edge_directions)
    `bound` `{` $bound_nodes `}` `pending` custom<ColumnIndices>($pending_nodes) `{` $bound_pending `}`
    `values` `{` $node_prop_values `}` `{` $edge_prop_values `}`
    `carrying` `{` $carried_columns `}`
    attr-dict `:` functional-type(operands, results)
  }];
}

def SetNodeProperty : TuringOp<"set_node_property", [AttrSizedOperandSegments]> {
  let summary = "Set one named property on each input node to a row-aligned value";

  let description = [{
      %n   = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %val = db.constant(5 : i64)
      db.set_node_property(%n, "age", %val)
        : (!db.column<!storage.node_id>, !db.column<i64>) -> ()

    Node variable keeps its existing column.
  }];

  let arguments = (ins Column:$input_nodes,
                       StrAttr:$property,
                       Column:$value,
                       Optional<Column>:$pending,
                       Optional<Column>:$rows,
                       UnitAttr:$all_pending);

  // `pending` says, row by row, which entities this change wrote and has not committed -
  // those are written in the write buffer rather than recorded as an update to a
  // committed entity, and `all_pending` says it of every row without a mask. `rows`
  // narrows which rows are written at all: Cypher's ON CREATE passes the merge's created
  // mask, ON MATCH its negation, and a plain SET passes none.
  let assemblyFormat = [{
    `(` $input_nodes `,` $property `,` $value `)` (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

def SetEdgeProperty : TuringOp<"set_edge_property", [AttrSizedOperandSegments]> {
  let summary = "Set one named property on each input edge to a row-aligned value";

  let description = [{
      db.set_edge_property(%e, "weight", %val)
        : (!db.column<!storage.edge_id>, !db.column<f64>) -> ()

    Edge variable keeps its existing column.
  }];

  let arguments = (ins Column:$input_edges,
                       StrAttr:$property,
                       Column:$value,
                       Optional<Column>:$pending,
                       Optional<Column>:$rows,
                       UnitAttr:$all_pending);

  let assemblyFormat = [{
    `(` $input_edges `,` $property `,` $value `)` (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

def SetNodeProperties : TuringOp<"set_node_properties", [AttrSizedOperandSegments]> {
  let summary = "Write the entries of a row-aligned map as the properties of each input node";

  let description = [{
      %n = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %m = db.constant({age = 32 : i64, name = "Remy" : !storage.string})
      db.set_node_properties(%n, %m) replaces
        : (!db.column<!storage.node_id>, !db.column<!storage.map>) -> ()

    Cypher's SET n = m, and SET n += m without `replaces`. Each entry of the row's map
    sets the property its key names, and an entry holding null removes it. `replaces`
    also removes every property the node holds that the map does not name. A row holding
    a null map writes an empty one. `creates` gives the element type of each property a
    map literal names and the graph may not hold yet.
  }];

  let arguments = (ins Column:$input_nodes,
                       Column:$value,
                       Optional<Column>:$pending,
                       Optional<Column>:$rows,
                       UnitAttr:$all_pending,
                       UnitAttr:$replaces,
                       OptionalAttr<DictionaryAttr>:$created_types);

  let assemblyFormat = [{
    `(` $input_nodes `,` $value `)` (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? (`replaces` $replaces^)? (`creates` $created_types^)?
    attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

def SetEdgeProperties : TuringOp<"set_edge_properties", [AttrSizedOperandSegments]> {
  let summary = "Write the entries of a row-aligned map as the properties of each input edge";

  let description = [{
      db.set_edge_properties(%e, %m) replaces
        : (!db.column<!storage.edge_id>, !db.column<!storage.map>) -> ()
  }];

  let arguments = (ins Column:$input_edges,
                       Column:$value,
                       Optional<Column>:$pending,
                       Optional<Column>:$rows,
                       UnitAttr:$all_pending,
                       UnitAttr:$replaces,
                       OptionalAttr<DictionaryAttr>:$created_types);

  let assemblyFormat = [{
    `(` $input_edges `,` $value `)` (`pending` $pending^)? (`rows` $rows^)?
    (`all_pending` $all_pending^)? (`replaces` $replaces^)? (`creates` $created_types^)?
    attr-dict `:` functional-type(operands, results)
  }];

  let hasVerifier = 1;
}

def DeleteNode : TuringOp<"delete_node"> {
  let summary = "Delete each input node, optionally detaching its incident edges";

  let description = [{
    `detach` carries the DETACH keyword

      db.delete_node(%n) detach : (!db.column<!storage.node_id>) -> ()

    `pending` is the mask a db.merge produced beside its node column: those rows
    name a node this change wrote by the ID it will commit as, so the change drops
    the write instead of tombstoning an ID.
    `all_pending` says that of every row, which a db.create_node's column is.
  }];

  let arguments = (ins Column:$input_nodes,
                       UnitAttr:$detach,
                       Optional<Column>:$pending,
                       UnitAttr:$all_pending);

  let assemblyFormat = [{
    `(` $input_nodes `)` (`detach` $detach^)? (`pending` $pending^)?
    (`all_pending` $all_pending^)? attr-dict `:` functional-type(operands, results)
  }];
}

def DeleteEdge : TuringOp<"delete_edge"> {
  let summary = "Delete each input edge";

  let description = [{
      db.delete_edge(%e) : (!db.column<!storage.edge_id>) -> ()

    `pending` is delete_node's mask, over edges, and `all_pending` its maskless form.
  }];

  let arguments = (ins Column:$input_edges, Optional<Column>:$pending, UnitAttr:$all_pending);

  let assemblyFormat = [{
    `(` $input_edges `)` (`pending` $pending^)? (`all_pending` $all_pending^)? attr-dict `:`
    functional-type(operands, results)
  }];
}

#endif // TURINGDB_WRITE_OPS
