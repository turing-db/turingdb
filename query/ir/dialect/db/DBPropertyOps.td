#ifndef TURINGDB_PROPERTY_OPS
#define TURINGDB_PROPERTY_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "StorageTypes.td"

/**
* GetNodeProperties
*/
def GetNodeProperties : TuringOp<"get_node_properties", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Read one named property of each node as a column of values";

  let description = [{
    In `MATCH (a) RETURN a.name` the node column produced by a scan or a
    traversal carries only node IDs; this op fetches the value behind `a.name`,
    one per node, into a new column:

      %a    = db.scan_nodes() : !db.column<!storage.node_id>
      %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>

    A node may not have the property, so missing values come back null and no
    node is dropped - the lowering uses the with-null property fetch. The value
    column is the property's value type made nullable, which the analyzer resolves
    against the schema and the properties the query itself creates. One op
    reads one property; `RETURN a.name, a.age` is two ops sharing %a. Reading a
    property is a deterministic read of the graph, so the op is Pure.

    `pending`, when present, is the mask a db.merge produced beside its node
    column: those rows name a node this change wrote by the ID it will commit as,
    past every node the graph holds, so their value is read out of the change's write
    buffer rather than off the graph - null there when the write set the property on
    none of them.

      %v = db.get_node_properties(%n, "age") pending %mask
             : (!db.column<!storage.node_id>, !db.column<!storage.bool>) -> !db.column<!storage.nullable<i64>>

    `all_pending` says the same of every row without a mask to say it row by row,
    which is what a db.create_node's column is: the read comes wholly out of the
    write buffer.

      %v = db.get_node_properties(%n, "age") all_pending
             : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
  }];

  // `property` is the symbolic property name, the same `name`/`age` spelled in
  // the query. Being an attribute and not an SSA value, it is absent from
  // `operands` below; lowering resolves it to a storage PropertyTypeID.
  let arguments = (ins Column:$input_nodes,
                       StrAttr:$property,
                       Optional<Column>:$pending,
                       UnitAttr:$all_pending);
  let results   = (outs ColumnNullables:$result);

  // The string attribute prints inside the parens, ahead of the colon, and MLIR
  // elides it from attr-dict because the format already names it. The optional
  // mask prints after a `pending` keyword, which anchors its group.
  let assemblyFormat = [{
    `(` $input_nodes `,` $property `)` (`pending` $pending^)? (`all_pending` $all_pending^)?
    attr-dict `:` functional-type(operands, results)
  }];
}

/**
* GetEdgeProperties
*/
def GetEdgeProperties : TuringOp<"get_edge_properties", [Pure, ConstantThroughOperands, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Read one named property of each edge as a column of values";

  let description = [{
    The edge counterpart of get_node_properties. In
    `MATCH (a)-[e]->(b) RETURN e.weight` the edge column carries only edge IDs;
    this op fetches the value behind `e.weight`, one per edge, with missing
    values null:

      %weight = db.get_edge_properties(%eids, "weight") : (!db.column<!storage.edge_id>) -> !db.column<!storage.nullable<i64>>

    The input column carries its element type - !storage.edge_id here - and the
    op chosen (get_node_properties vs get_edge_properties) selects which side of
    the graph the property is read from. The value column is typed as
    get_node_properties' is.

    `pending` is get_node_properties' mask, over edges: a row a db.merge wrote
    names the edge by the ID it will commit as, and its value is read out of the
    write buffer.
    `all_pending` is its maskless form, which a db.create_edge's column is.
  }];

  let arguments = (ins Column:$input_edges,
                       StrAttr:$property,
                       Optional<Column>:$pending,
                       UnitAttr:$all_pending);
  let results   = (outs ColumnNullables:$result);

  let assemblyFormat = [{
    `(` $input_edges `,` $property `)` (`pending` $pending^)? (`all_pending` $all_pending^)?
    attr-dict `:` functional-type(operands, results)
  }];
}

def GetNodeLabelSet : TuringOp<"get_node_label_set", [Pure, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Read the label-set ID of each node as a column";

  let description = [{
    For each node in the input column, fetches its LabelSetID.

      %ls = db.get_node_label_set(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
      %ok = db.check_label_constraint(%ls, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  }];

  let arguments = (ins Column:$input_nodes);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input_nodes `)` attr-dict `:` functional-type(operands, results)
  }];
}

def GetEdgeTypes : TuringOp<"get_edge_types", [Pure, DeclareOpInterfaceMethods<RowCostOpInterface>]> {
  let summary = "Read the type ID of each edge as a column";

  let description = [{
    The edge counterpart of get_node_label_set: for each edge in the input column,
    fetches its EdgeTypeID. A traversal already publishes this column beside the edges
    it walks, so this reads the type of edges that reach it any other way - the ones a
    barrier republished, whose type column the barrier did not carry.

      %et = db.get_edge_types(%e) : (!db.column<!storage.edge_id>) -> !db.column<!storage.edge_type_id>
      %ok = db.check_edge_type_constraint(%et, ["KNOWS"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  }];

  let arguments = (ins Column:$input_edges);
  let results   = (outs Column:$result);

  let assemblyFormat = [{
    `(` $input_edges `)` attr-dict `:` functional-type(operands, results)
  }];
}

def CheckLabelConstraint : TuringOp<"check_label_constraint", [Pure]> {
  let summary = "Test each label-set ID against a required-label constraint";

  let description = [{
    For each LabelSetID in the input column, emits true if the node's label set
    contains every label of at least one of `alternatives` (a superset match), false
    otherwise. A single alternative is written flat, several are written nested.

      %ok = db.check_label_constraint(%ls, ["Person", "Employee"])
              : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
      %ok = db.check_label_constraint(%ls, [["Person", "Employee"], ["Interest"]])
              : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>

    With `nullable`, a row whose node is null - one an OPTIONAL MATCH missed - tests
    null rather than false, as a label test written in an expression must.
  }];

  let arguments = (ins Column:$labelset_ids, LabelRefArrayArrayAttr:$alternatives, UnitAttr:$nullable);
  let results   = (outs Column:$result);

  let hasVerifier = 1;

  let extraClassDeclaration = [{
    // The labels of the sole alternative, or null when there are several
    ::mlir::ArrayAttr getConjunction();
  }];

  let assemblyFormat = [{
    `(` $labelset_ids `,` custom<LabelAlternatives>($alternatives) `)` attr-dict `:` functional-type(operands, results)
  }];
}

def CheckEdgeTypeConstraint : TuringOp<"check_edge_type_constraint", [Pure]> {
  let summary = "Test each edge type ID against a required-type constraint";

  let description = [{
    For each EdgeTypeID in the input column, emits true if the edge's type
    matches any of the types in `edge_types` (OR semantics), false otherwise.

      %ok = db.check_edge_type_constraint(%et, ["KNOWS", "LIKES"])
              : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>

    With `nullable`, a row whose edge is null tests null rather than false, as for
    db.check_label_constraint.
  }];

  let arguments = (ins Column:$edge_type_ids, EdgeTypeRefArrayAttr:$edge_types, UnitAttr:$nullable);
  let results   = (outs Column:$result);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $edge_type_ids `,` $edge_types `)` attr-dict `:` functional-type(operands, results)
  }];
}

#endif // TURINGDB_PROPERTY_OPS
