#ifndef TURINGDB_SCAN_OPS
#define TURINGDB_SCAN_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/OpAsmInterface.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "StorageTypes.td"

/**
* ScanNodes
*/
def ScanNodes : TuringOp<"scan_nodes", [Pure]> {
  let summary = "Produce all nodes in the graph";
  let results   = (outs ColumnNodeIDs:$result);
  // `qualified(...)` keeps the full `!db.column<...>` mnemonic on the result type
  // rather than the stripped `<...>` form, matching how get_out_edges prints.
  let assemblyFormat = "`(`operands`)` attr-dict `:` qualified(type($result))";
}

/**
* ScanNodesByLabel
*/
def ScanNodesByLabel : TuringOp<"scan_nodes_by_label", [Pure]> {
  let summary = "Produce the nodes in the graph carrying every given label";

  let description = [{
    In `MATCH (a:Person)` the node column is not every node but only those
    carrying the Person label; `MATCH (a:Person:Employee)` narrows further to
    the nodes carrying both:

      %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
      %b = db.scan_nodes_by_label(["Person", "Employee"]) : !db.column<!storage.node_id>

    `labels` is that set of label names, spelled the same as in the query. A
    node qualifies when its own label set contains every requested label (a
    superset match), so a node labelled `Person:Employee:Manager` still matches
    `["Person", "Employee"]`. It follows that if any requested label was never
    created on a node - it is absent from the schema - the conjunction matches
    nothing and the scan yields no rows. Lowering resolves each name to a
    storage LabelID. Scanning the graph is a deterministic read, so the op is
    Pure.

    The label list must not be empty: a label-free scan is `db.scan_nodes`.
  }];

  // `labels` is the set of symbolic label names. Being an attribute and not an
  // SSA value, it is absent from `operands`; lowering resolves each name to a
  // storage LabelID.
  let arguments = (ins LabelRefArrayAttr:$labels);
  let results   = (outs ColumnNodeIDs:$result);
  let hasVerifier = 1;

  // The label list prints inside the parens as `["Person", "Employee"]`, ahead
  // of the colon, the same place db.get_node_properties prints its name; the
  // result type follows, qualified, as db.scan_nodes prints it.
  let assemblyFormat = "`(` $labels `)` attr-dict `:` qualified(type($result))";
}

/**
* ConstScanNodes
*/
def ConstScanNodes : TuringOp<"const_scan_nodes", [Pure]> {
  let summary = "Produce a fixed, explicitly listed set of nodes";

  let description = [{
    The constant-set counterpart of db.scan_nodes: where that produces every node
    in the graph, this produces exactly the nodes whose IDs are listed on the op -
    a set fixed before the scan starts rather than discovered by walking the graph:

      %a = db.const_scan_nodes([0, 3, 7]) : !db.column<!storage.node_id>

    It seeds a dataflow from a known set of nodes - the declarative counterpart of
    the plan's ConstScanNode, which turns a node-ID membership predicate
    (MATCH (n) WHERE id(n) IN [0, 3, 7]) into a direct scan of those IDs rather
    than a full scan followed by a filter. Like db.scan_nodes it reads no row and
    takes no input, so it opens a dataflow rather than consuming one; the IDs are
    emitted in the order listed. Producing a fixed list is deterministic, so the
    op is Pure.

    `nodeIDs` lists the IDs the query spells. `boundIDs` are the IDs bound when the
    query runs, each a column holding one value for the whole run: a node, an
    integer, or a list whose numbers are the IDs. A parameter is one, and so is a
    value ops that cannot raise compute from parameters and literals alone, as
    `$id + 1`, whose column may leave its number for lowering to type.
    `id(n) = $id OR id(n) IN $ids` scans

      %m = db.entity_id_of(%id) : (!db.column<f64>) -> !db.column<!storage.node_id>
      %a = db.const_scan_nodes([], %m, %ids : !db.column<!storage.node_id>, !db.column<!storage.list<!storage.any>>)
        : !db.column<!storage.node_id>

    The scan reads the bound IDs when it starts, so the program does not depend on
    the values bound. With bound IDs it emits the nodes the listed and the bound
    IDs name, each once, in ID order.

    A node ID is a non-negative handle, so the signless i64 dense array matches
    the index list db.sort carries; each entry is a storage NodeID once lowered.
    An ID that names no node in the graph matches nothing, so it is dropped and the
    scan yields only the nodes that exist - the same set-membership semantics as
    the WHERE id(n) IN ... it models. A bound integer names the node whose ID it
    equals, and so does a number of a bound list, an integer or a double holding
    one. Any other value - a null, a negative or fractional number, a string, a list
    inside the list - names no node. No ID at all yields no row - not an error,
    matching that same set-membership.
  }];

  let arguments = (ins DenseI64ArrayAttr:$nodeIDs, Variadic<Column>:$boundIDs);
  let results   = (outs ColumnNodeIDs:$result);
  let hasVerifier = 1;

  let builders = [
    OpBuilder<(ins "::mlir::Type":$result, "::llvm::ArrayRef<int64_t>":$nodeIDs), [{
      build($_builder, $_state, result, nodeIDs, ::mlir::ValueRange());
    }]>
  ];

  // The node ID list prints inside the parens as `[0, 3, 7]`, ahead of the colon,
  // the same place db.scan_nodes_by_label prints its labels, followed by the bound
  // IDs and their types; the result type follows, qualified, as db.scan_nodes
  // prints it.
  let assemblyFormat = [{
    `(` $nodeIDs (`,` $boundIDs^ `:` type($boundIDs))? `)` attr-dict `:` qualified(type($result))
  }];
}

/**
* FetchNodes
*/
def FetchNodes : TuringOp<"fetch_nodes", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet"]>]> {
  let summary = "Produce for each row the node its ID cell names, dropping the rows naming none";

  let description = [{
    The per-row counterpart of db.const_scan_nodes: the IDs are a column computed for
    each row rather than a list known at plan time.

      %a, %i2 = db.fetch_nodes(%ids, {%i}) : (!db.column<none>, !db.column<none>)
                                         -> (!db.column<!storage.node_id>, !db.column<none>)

    A row is kept where its cell equals a node a scan would produce - one the graph
    holds or the query created - as `n = cell` would hold of it: a node, an integer
    holding the node's ID, or a tagged cell holding either. Every other row is dropped -
    a null, a value of another type, a negative integer, an ID naming no node or a
    deleted one.
    `columns_to_filter` is the carry set, cut to the kept rows as db.filter cuts it.
  }];

  let arguments = (ins Column:$ids, Variadic<Column>:$columns_to_filter);
  let results   = (outs ColumnNodeIDs:$nodes, Variadic<Column>:$carried);

  let assemblyFormat = [{
    `(` $ids `,` `{` $columns_to_filter `}` `)` attr-dict `:` functional-type(operands, results)
  }];
}

/**
* ConstScanEdges
*/
def ConstScanEdges : TuringOp<"const_scan_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>]> {
  let summary = "Produce a fixed, explicitly listed set of edges as four row-aligned columns";

  let description = [{
    The edge counterpart of db.const_scan_nodes: where db.scan_edges produces every edge,
    this produces exactly the edges whose IDs it lists, in the same four row-aligned columns
    - the source node, the edge, its type and the target node:

      %srcs, %eids, %etypes, %tgts = db.const_scan_edges([3, 7]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is what a pattern filtered on the ID of one of its edges is walked out from
    (reroot_pattern_at_edge_ids): `MATCH (a)-[r]->(b) WHERE id(r) IN [3, 7]` looks the two
    edges up rather than walking every edge.

    `edgeIDs` and `boundIDs` hold the IDs as db.const_scan_nodes holds them: the ones the
    query spells, and the ones bound when the query runs, each a column holding one value
    for the run - an edge, an integer, or a list whose numbers are the IDs. The scan reads
    the bound IDs when it starts and emits the edges the IDs name, each once, in ID order.
    An ID naming no edge a scan produces names none, as a bound value that is not a number
    or a negative number does.

    With `undirected` each edge comes out twice, once from each end, as db.get_edges walks
    it from every node: the first column holds the end it is walked from and the last one
    the other end, so a self-loop comes out twice. `MATCH (a)-[r]-(b)` reads that form.

    Like db.scan_edges it reads no row and takes no input, so it opens a dataflow rather
    than consuming one. Producing a fixed list is deterministic, so the op is Pure.
  }];

  let arguments = (ins DenseI64ArrayAttr:$edgeIDs, Variadic<Column>:$boundIDs, UnitAttr:$undirected);
  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $edgeIDs (`,` $boundIDs^ `:` type($boundIDs))? `)` (`undirected` $undirected^)? attr-dict `:`
    qualified(type($srcids)) `,` qualified(type($eids)) `,` qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* FetchEdges
*/
def FetchEdges : TuringOp<"fetch_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet"]>]> {
  let summary = "Produce for each row the edge its ID cell names, dropping the rows naming none";

  let description = [{
    The edge counterpart of db.fetch_nodes, and the per-row counterpart of
    db.const_scan_edges: the IDs are a column computed for each row rather than a list known
    at plan time.

      %s, %e, %t, %d, %i2 = db.fetch_edges(%ids, {%i})
          : (!db.column<!storage.edge_id>, !db.column<none>)
         -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<none>)

    A row is kept where its cell names an edge a scan would produce - one the graph holds or
    the query created - as `r = cell` would hold of it: an edge, an integer holding the
    edge's ID, or a tagged cell holding either. The edge comes out in the four columns of
    db.scan_edges, and with `undirected` twice, once from each end, as db.const_scan_edges
    orients them. Every other row is dropped. `columns_to_filter` is the carry set, cut to
    the kept rows as db.filter cuts it.
  }];

  let arguments = (ins Column:$ids, Variadic<Column>:$columns_to_filter, UnitAttr:$undirected);
  let results   = (outs ColumnNodeIDs:$srcids,
                        ColumnEdgeIDs:$eids,
                        ColumnEdgeTypes:$etypes,
                        ColumnNodeIDs:$tgtids,
                        Variadic<Column>:$carried);

  let assemblyFormat = [{
    `(` $ids `,` `{` $columns_to_filter `}` `)` (`undirected` $undirected^)? attr-dict `:` functional-type(operands, results)
  }];
}

/**
* ScanNodesByPropertyValue
*/
def ScanNodesByPropertyValue : TuringOp<"scan_nodes_by_property_value", [Pure]> {
  let summary = "Produce the nodes whose named property equals a value";

  let description = [{
    The property-equality counterpart of db.scan_nodes_by_label: where that
    produces the nodes carrying a label, this produces the nodes whose named
    property holds exactly the given value, optionally among the nodes
    carrying every listed label:

      %a = db.scan_nodes_by_property_value("age", 32 : i64) : !db.column<!storage.node_id>
      %b = db.scan_nodes_by_property_value("age", 32 : i64, ["Person"]) : !db.column<!storage.node_id>

    It is what a `WHERE n.age = 32` filter over a plain or labelled scan fuses
    into (FuseScanByPropertyValue): rather than scanning every node, fetching the
    property of each and masking, the scan walks each part's property column in
    place - only the ranges of the matching label sets when labels are given -
    and emits only the matching node IDs. A node that lacks the property never
    appears, exactly as the filter dropped it on a null value.

    `property` is the property name as spelled in the query. The value is either
    `value`, the literal the query spells - the typed attribute db.constant
    carried, one of the literal kinds the query language can spell: a signless
    i64 integer, an f64 double, an i1 boolean or a !storage.string - or
    `boundValue`, a column of one of those types holding one value for the whole
    run, which the scan reads when it starts, and which matches no node where it
    holds null. A parameter is one, and so is a value ops that cannot raise compute
    from parameters and literals alone, as `$first + $last`:

      %c = db.scan_nodes_by_property_value("age", %age : !db.column<i64>) : !db.column<!storage.node_id>

    Exactly one of the two is given. Lowering forwards both as-is; translation
    resolves the name against the schema and the scan coerces the value to the
    stored type. A name the schema does not hold, or a value no stored value of
    that property could equal, deliberately scans nothing rather than raising:
    the fusion is more permissive than the filter form, which throws on both.
    Neither shape reaches here from Cypher - the analyzer rejects an unknown
    property and a mismatched literal type before codegen. `labels`, when
    present, is the label list db.scan_nodes_by_label carried, with the same
    superset semantics: a label absent from the schema likewise yields no rows.

    Like db.scan_nodes it reads no row and takes no input, so it opens a
    dataflow rather than consuming one. Scanning the graph is a deterministic
    read, so the op is Pure.
  }];

  let arguments = (ins StrAttr:$property,
                       OptionalAttr<TypedAttrInterface>:$value,
                       Optional<Column>:$boundValue,
                       OptionalAttr<LabelRefArrayAttr>:$labels);
  let results   = (outs ColumnNodeIDs:$result);
  let hasVerifier = 1;

  // The name, value and optional labels print inside the parens as
  // `"age", 32 : i64, ["Person"]`, ahead of the colon, the same place
  // db.get_node_properties prints its name; the result type follows, qualified,
  // as db.scan_nodes prints it.
  let assemblyFormat = [{
    `(` $property `,` custom<PropertyScanValue>($value, $boundValue, type($boundValue)) (`,` $labels^)? `)` attr-dict `:` qualified(type($result))
  }];
}

/**
* ScanEdges
*/
def ScanEdges : TuringOp<"scan_edges", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface]> {
  let summary = "Produce every edge in the graph as four row-aligned columns";

  let description = [{
    The edge counterpart of db.scan_nodes: where that produces every node, this
    produces every edge, expanded into the same four row-aligned columns a hop
    exposes - the source node, the edge, its type and the target node:

      %srcs, %eids, %etypes, %tgts = db.scan_edges() : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It reads no column and takes no input, so - like db.scan_nodes - it opens a
    dataflow rather than consuming one. Unlike db.get_out_edges there is no source
    node column to walk from and no carry set to filter: every edge is emitted
    once. The four result chunks are the same shape and order db.get_out_edges
    produces (sources, edge IDs, edge type IDs, targets), so a downstream op reads
    them the same way. Scanning the graph is a deterministic read, so the op is
    Pure.
  }];

  // No operands: a scan opens the dataflow. The four results are the source
  // nodes, the edges, their types and the target nodes, in the same order
  // db.get_out_edges produces them.
  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);

  // A source op has no meaningful operand arrow, so - like db.scan_nodes - the
  // result types are spelled straight after the colon rather than through a
  // functional-type; qualified(...) keeps the leading !db. on each.
  let assemblyFormat = [{
    `(`operands`)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanEdgesByType
*/
def ScanEdgesByType : TuringOp<"scan_edges_by_type", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface]> {
  let summary = "Produce every edge of the named types in the graph as four row-aligned columns";

  let description = [{
    The by-type sibling of db.scan_edges: where that produces every edge, this
    produces every edge carrying one of the named types, in the same four
    row-aligned columns - the source node, the edge, its type and the target node:

      %srcs, %eids, %etypes, %tgts = db.scan_edges_by_type(["KNOWS"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    An edge carries exactly one type, so `edge_types` is a disjunction over the
    types the pattern named, as `-[:KNOWS|LIKES]->` spells it: an edge matches when
    it carries any one of them. The names are spelled the same as in the query and
    resolved against the graph schema; one absent from the schema drops out, and a
    set nothing resolved into matches no edge, so the scan yields no rows. An empty
    `edge_types` says that outright - a pass narrowing the read to nothing writes one -
    and the scan then walks no edge at all. Like
    db.scan_edges it reads no column and takes no input, so it opens a dataflow
    rather than consuming one, and the result shape is identical - only the rows
    are narrowed.
  }];

  // `edge_types` are the symbolic relationship-type names. Being an attribute and
  // not an SSA value, the op still has no operands; lowering hoists them into an
  // nl.get_edge_type_set handle so they are resolved once above the loops.
  let arguments = (ins EdgeTypeRefArrayAttr:$edge_types);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);

  // The type names print inside the parens where db.scan_edges prints nothing;
  // the result types follow the colon, as on db.scan_edges, since a source op has
  // no meaningful operand arrow.
  let assemblyFormat = [{
    `(` $edge_types `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanOutEdgesByLabelSrc
*/
def ScanOutEdgesByLabelSrc : TuringOp<"scan_out_edges_by_label_src", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface]> {
  let summary = "Produce every out-edge of the nodes carrying every given label as four row-aligned columns";

  let description = [{
    The by-label sibling of db.scan_edges: where that produces every edge, this
    produces the edges leaving the nodes whose label set is a superset of `labels`,
    in the same four row-aligned columns - the source node, the edge, its type and
    the target node:

      %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_src(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a:Person)-->(b)` read off the edge index rather than walked: the
    same edges a db.scan_nodes_by_label feeding a db.get_out_edges produces, in one
    op that never builds the node column between them. `labels` is the label set
    db.scan_nodes_by_label carried, matched the same way - a node labelled
    `Person:Employee` still matches `["Person"]`, and a label absent from the schema
    leaves the conjunction unsatisfiable, so the scan yields no rows. Like
    db.scan_edges it reads no column and takes no input, so it opens a dataflow
    rather than consuming one.

    The label list must not be empty: a label-free scan of every edge is
    db.scan_edges.
  }];

  // `labels` is the set of symbolic label names the source nodes must carry. Being
  // an attribute and not an SSA value, the op still has no operands; lowering
  // resolves each name to a storage LabelID.
  let arguments = (ins LabelRefArrayAttr:$labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  // The label list prints inside the parens where db.scan_edges_by_type prints its
  // type name; the result types follow the colon, as on db.scan_edges, since a
  // source op has no meaningful operand arrow.
  let assemblyFormat = [{
    `(` $labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanInEdgesByLabelTgt
*/
def ScanInEdgesByLabelTgt : TuringOp<"scan_in_edges_by_label_tgt", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface]> {
  let summary = "Produce every in-edge of the nodes carrying every given label as four row-aligned columns";

  let description = [{
    The reverse-direction sibling of db.scan_out_edges_by_label_src: where that
    produces the edges leaving the nodes whose label set is a superset of `labels`,
    this produces the edges arriving at them, in the same four row-aligned columns:

      %srcs, %eids, %etypes, %tgts = db.scan_in_edges_by_label_tgt(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a:Person)<--(b)` read off the edge index rather than walked: the
    same edges a db.scan_nodes_by_label feeding a db.get_in_edges produces. The
    labels constrain the node the walk arrives at, which is the edge's target, so
    `srcids` holds the unconstrained end and `tgtids` the labelled one - the reverse
    of the out-edge scan, where the labelled end is the source. Both columns still
    hold the edge as the graph stores it, exactly as db.get_in_edges fills them, so
    a consumer reads them the same way whichever scan produced them.

    The label list must not be empty: a label-free scan of every edge is
    db.scan_edges.
  }];

  let arguments = (ins LabelRefArrayAttr:$labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanOutEdgesByLabelTgt
*/
def ScanOutEdgesByLabelTgt : TuringOp<"scan_out_edges_by_label_tgt", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface]> {
  let summary = "Produce every out-edge arriving at the nodes carrying every given label as four row-aligned columns";

  let description = [{
    The other endpoint's sibling of db.scan_out_edges_by_label_src: there the labels
    constrain the node the hop leaves, here the one it arrives at, which is the edge's
    target. It produces the edges whose target's label set is a superset of `labels`,
    in the same four row-aligned columns:

      %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_tgt(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a)-->(b:Person)` read off the edge index rather than scanned and
    filtered: the same edges a db.scan_edges keeps once a db.check_label_constraint
    over its target column has dropped the rest. `srcids` holds the unconstrained end
    and `tgtids` the labelled one. Those are the rows db.scan_in_edges_by_label_tgt
    produces, since both read the index keyed by the target's label set; the two are
    told apart by the hop the query wrote, not by what they emit.

    The label list must not be empty: a label-free scan of every edge is
    db.scan_edges.
  }];

  let arguments = (ins LabelRefArrayAttr:$labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanInEdgesByLabelSrc
*/
def ScanInEdgesByLabelSrc : TuringOp<"scan_in_edges_by_label_src", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface]> {
  let summary = "Produce every in-edge leaving the nodes carrying every given label as four row-aligned columns";

  let description = [{
    The other endpoint's sibling of db.scan_in_edges_by_label_tgt: there the labels
    constrain the node the in-hop arrives at, here the one it leaves, which is the
    edge's source. It produces the edges whose source's label set is a superset of
    `labels`:

      %srcs, %eids, %etypes, %tgts = db.scan_in_edges_by_label_src(["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a)<--(b:Person)` read off the edge index rather than scanned and
    filtered: the same edges a db.scan_edges keeps once a db.check_label_constraint
    over its source column has dropped the rest. `srcids` holds the labelled end and
    `tgtids` the unconstrained one. Those are the rows db.scan_out_edges_by_label_src
    produces, since both read the index keyed by the source's label set.

    The label list must not be empty: a label-free scan of every edge is
    db.scan_edges.
  }];

  let arguments = (ins LabelRefArrayAttr:$labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanOutEdgesByLabelSrcTgt
*/
def ScanOutEdgesByLabelSrcTgt : TuringOp<"scan_out_edges_by_label_src_tgt", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface]> {
  let summary = "Produce every edge whose source and target each carry every given label of their end as four row-aligned columns";

  let description = [{
    The both-ends sibling of db.scan_out_edges_by_label_src: it produces the edges
    whose source's label set is a superset of `src_labels` and whose target's is a
    superset of `tgt_labels`, in the same four row-aligned columns:

      %srcs, %eids, %etypes, %tgts = db.scan_out_edges_by_label_src_tgt(["Person"], ["Interest"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a:Person)-->(b:Interest)`: the edges db.scan_out_edges_by_label_src
    keeps once a db.check_label_constraint over its target column has dropped the
    rest. It reads the index keyed by the source's label set, as that scan does, and
    checks each edge's target against `tgt_labels`. db.scan_in_edges_by_label_src_tgt
    produces the same rows off the index keyed by the target's label set.

    Neither label list may be empty: a scan labelled on one end only is
    db.scan_out_edges_by_label_src or db.scan_in_edges_by_label_tgt.
  }];

  let arguments = (ins LabelRefArrayAttr:$src_labels, LabelRefArrayAttr:$tgt_labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $src_labels `,` $tgt_labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

/**
* ScanInEdgesByLabelSrcTgt
*/
def ScanInEdgesByLabelSrcTgt : TuringOp<"scan_in_edges_by_label_src_tgt", [Pure, DeclareOpInterfaceMethods<OpAsmOpInterface, ["getAsmResultNames"]>, EdgeTypesOutputOpInterface]> {
  let summary = "Produce every edge whose target and source each carry every given label of their end, read off the target's index";

  let description = [{
    The both-ends sibling of db.scan_in_edges_by_label_tgt: it produces the edges
    whose source's label set is a superset of `src_labels` and whose target's is a
    superset of `tgt_labels`:

      %srcs, %eids, %etypes, %tgts = db.scan_in_edges_by_label_src_tgt(["Interest"], ["Person"]) : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    It is `MATCH (a:Person)<--(b:Interest)`: the edges db.scan_in_edges_by_label_tgt
    keeps once a db.check_label_constraint over its source column has dropped the
    rest. It reads the index keyed by the target's label set, as that scan does, and
    checks each edge's source against `src_labels`. The rows are those of
    db.scan_out_edges_by_label_src_tgt; the two differ in the index they read.

    Neither label list may be empty: a scan labelled on one end only is
    db.scan_in_edges_by_label_tgt or db.scan_out_edges_by_label_src.
  }];

  let arguments = (ins LabelRefArrayAttr:$src_labels, LabelRefArrayAttr:$tgt_labels);

  let results = (outs ColumnNodeIDs:$srcids,
                      ColumnEdgeIDs:$eids,
                      ColumnEdgeTypes:$etypes,
                      ColumnNodeIDs:$tgtids);
  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $src_labels `,` $tgt_labels `)` attr-dict `:` qualified(type($srcids)) `,` qualified(type($eids)) `,`
    qualified(type($etypes)) `,` qualified(type($tgtids))
  }];
}

#endif // TURINGDB_SCAN_OPS
