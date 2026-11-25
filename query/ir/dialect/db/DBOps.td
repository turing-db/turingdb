#ifndef TURINGDB_OPS
#define TURINGDB_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"
include "mlir/IR/OpAsmInterface.td"
include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/IR/BuiltinAttributeInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "DBSystemOps.td"
include "DBScanOps.td"
include "DBHopOps.td"
include "DBPathOps.td"
include "DBSourceOps.td"
include "DBPropertyOps.td"
include "DBListOps.td"
include "DBFunctionOps.td"
include "DBExpressionOps.td"
include "DBRowOps.td"
include "DBJoinOps.td"
include "DBAggregateOps.td"

/**
* PatternComprehension
*/
def PatternComprehension : TuringOp<"pattern_comprehension", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getRegionCarrySet"]>]> {
  let summary = "Build one list per row out of the matches a pattern finds for that row";

  let description = [{
    Cypher's `[(a)-[:KNOWS]->(b) WHERE p(b) | f(b)]`. The op matches a pattern once per row
    in flight and produces one list cell per row - the values the matches of that row
    contribute - so the rows in flight are the ones it started with, as
    db.list_comprehension leaves them:

      %names = db.pattern_comprehension(%a, %c) ({
        ^bb0(%aIn: !db.column<!storage.node_id>, %cIn: !db.column<none>, %tag: !db.column<ui64>):
          %s, %e, %et, %b, %tag2 = db.get_out_edges(%aIn, {%tag}) : ...
          %name = db.get_node_properties(%b) {property = "name"} : ...
          db.comprehension_yield %tag2, %name : !db.column<none>
      }) : (!db.column<!storage.node_id>, !db.column<none>) -> !db.column<none>

    `input_columns` are every column in flight where the comprehension is read, and the
    region's entry block takes one argument per input column plus a trailing
    `!db.column<ui64>` row tag - the position of each input row, carried through the
    pattern the way any other column is, so the row each match belongs to is known once
    the pattern is walked. The pattern joins onto the columns whose variables it names and
    carries the rest along, exactly as db.optional_match's does; a comprehension read
    where no column is in flight - `RETURN [(a:Person)-->(b) | b.name]` - has no input
    column and so no tag either, and its one list covers the single empty row Cypher
    starts from.

    The WHERE is a db.filter inside the region, so a match it drops is one the projection
    is never computed for, and it cuts the tag alongside the match. The db.comprehension_yield
    names the tag as the pattern left it and the value each match contributes.

    The result's type is the body's to decide and is resolved during lowering, as
    db.list_comprehension's is: the list is of whatever the body yields. A row the pattern
    matches nothing for gets the empty list, never a null - a pattern comprehension is the
    one list-valued expression that cannot be null.
  }];

  let arguments = (ins Variadic<Column>:$input_columns);
  let results   = (outs Column:$result);

  // Exactly one block, ending in a db.comprehension_yield.
  let regions = (region SizedRegion<1>:$pattern);

  let hasVerifier = 1;

  let assemblyFormat = [{
    `(` $input_columns `)` $pattern attr-dict `:` functional-type($input_columns, results)
  }];
}

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

/**
* OptionalMatch
*/
def OptionalMatch : TuringOp<"optional_match", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns", "getRegionCarrySet"]>]> {
  let summary = "Match a pattern against the rows in flight, keeping the rows it does not match";

  let description = [{
    Models Cypher's OPTIONAL MATCH: a left outer join of the rows in flight
    against a pattern. A row the pattern matches fans out exactly as a MATCH
    would; a row it matches not at all survives once, with the pattern's own
    variables null. The pattern matches whole or not at all - a second hop that
    finds nothing nulls the first hop's variables too - so it cannot be modelled
    by making each traversal optional, and lives in a region of its own.

    `input_columns` are the columns the pattern joins onto - every named column
    the query holds at this point. The region's entry block takes one argument
    per input column, so the pattern walks the rows in flight rather than the
    whole graph, plus a trailing `!db.column<ui64>` row tag: the position of each
    input row, carried through the pattern the way any other column is, so the
    rows that matched are known once the pattern is walked. A query opening on
    OPTIONAL MATCH has no input column and so no tag either - the rows it joins
    onto are the single empty row Cypher starts from, which any match marks.

      %p2, %e, %f = db.optional_match(%p) ({
        ^bb0(%pIn: !db.column<!storage.node_id>, %tag: !db.column<ui64>):
          %s, %e, %et, %t, %tag2 = db.get_out_edges(%pIn, {%tag}) : ...
          db.optional_yield %tag2, {%s, %e, %t} : !db.column<ui64>, {...}
      }) : (!db.column<!storage.node_id>) -> (...)

    The db.optional_yield names the tag as the pattern left it, then the columns
    the region contributes: one per input column first, in operand order - these
    are the input rows as the pattern filtered and repeated them - then one per
    variable the pattern binds of its own. The op's results line up with those
    columns, so the results past the input count are the pattern's variables, and
    those are the ones a row the pattern missed comes back with null. A null node
    or edge is an invalid ID (`ID::isValid()`), which reads as null through a
    property fetch and renders as null, so no column type changes here.

    sink_into_optional_match moves the size of a variable's list, and nodes(),
    relationships() or length() of its path, into the pattern: each is null when the
    variable is, so the padding carries its result rather than the list or path.
    trim_unread_columns drops a variable nothing past the op reads.

    Reading the graph is deterministic and the op writes nothing, so it is Pure -
    which says nothing about the row count, and this op changes it.
  }];

  let arguments = (ins Variadic<Column>:$input_columns);

  // One per column the region yields: the input columns as the pattern left
  // them, then the variables the pattern binds.
  let results = (outs Variadic<Column>:$results);

  // Exactly one block, ending in a db.optional_yield.
  let regions = (region SizedRegion<1>:$pattern);

  let assemblyFormat = [{
    `(` $input_columns `)` $pattern attr-dict `:` functional-type($input_columns, results)
  }];

  let hasVerifier = 1;
}

/**
* OptionalYield
*/
def OptionalYield : TuringOp<"optional_yield", [Pure, Terminator, AttrSizedOperandSegments, HasParent<"OptionalMatch">]> {
  let summary = "Name the row tag and the columns a db.optional_match's pattern contributes";

  let description = [{
    Terminates a db.optional_match's pattern region. `tag` is the row tag as the
    pattern left it - the input row each matched row came from, absent when the
    op has no input column - and `columns` are the columns the region
    contributes, one per input column in operand order followed by the pattern's
    own variables. The enclosing op's results are exactly those columns.
  }];

  let arguments = (ins Optional<ColumnRowTags>:$tag, Variadic<Column>:$columns);

  // The tag's type is buildable and so left out; only the contributed column types
  // are spelled, in braces as db.cross_product spells a factor's.
  let assemblyFormat = [{
    ($tag^ `,`)? `{` $columns `}` attr-dict `:` `{` qualified(type($columns)) `}`
  }];
}

/**
* CallSubquery
*/
def CallSubquery : TuringOp<"call_subquery", [DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns", "getRegionCarrySet", "getYieldedResultOffset"]>]> {
  let summary = "Run a query of its own once per row in flight";

  let description = [{
    Models Cypher's CALL { ... } subquery. `input_columns` are every column the
    query holds at this point, and the body region takes one argument per input
    column - the imported ones read through their own names, the rest carried past
    the body - plus a trailing `!db.column<ui64>` row tag when the op is `optional`
    and the body carries its scope over input rows it has, as db.optional_match
    takes one. The body is a self-contained db dataflow ending in a
    db.subquery_yield.

    The body runs once per input row. For each row a returning body yields, the op
    produces that row's input columns beside the yielded ones: the results are one
    per input column, in operand order, then one per yielded column. A body the row
    yields nothing for drops the row, unless the op is `optional`, which keeps it
    once with the yielded columns null.

      %p2, %x = db.call_subquery(%p) carries_scope ({
        ^bb0(%pIn: !db.column<!storage.node_id>):
          %s, %e, %et, %x = db.get_out_edges(%pIn, {}) : ...
          db.subquery_yield %s, %x : !db.column<!storage.node_id>, !db.column<!storage.node_id>
      }) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)

    `carries_scope` says the body's own dataflow keeps every input row paired with
    what the body made of it, so it runs over a whole chunk of input rows at once. A
    returning body that carries the scope hands the input columns back itself: it
    yields them first, as its dataflow left them - filtered and repeated by its hops -
    then its own columns, and the results line up with that yield. A body whose rows
    cannot carry the inputs through - one that aggregates, sorts, cuts or dedups - is
    run one input row at a time instead: a returning one yields its own columns alone
    and the lowering pairs each row it yields with the input row it came from, a unit
    one writes over one input row per step.

    A `unit` body ends on an updating clause rather than a RETURN. It yields nothing
    and the op has no results, whether or not it carries the scope: the rows in flight
    are the operands still, however many rows the body walked or wrote for each of
    them. A unit body is never `optional` - it yields no row for OPTIONAL to pad.

    The body may write to the graph, so the op is not Pure.
  }];

  let arguments = (ins Variadic<Column>:$input_columns,
                       UnitAttr:$unit,
                       UnitAttr:$carries_scope,
                       UnitAttr:$optional);

  let results = (outs Variadic<Column>:$results);

  // Exactly one block, ending in a db.subquery_yield.
  let regions = (region SizedRegion<1>:$body);

  let assemblyFormat = [{
    `(` $input_columns `)` (`unit` $unit^)? (`carries_scope` $carries_scope^)? (`optional` $optional^)?
    $body attr-dict `:` functional-type($input_columns, results)
  }];

  let hasVerifier = 1;
}

/**
* SubqueryYield
*/
def SubqueryYield : TuringOp<"subquery_yield", [Pure, Terminator, AttrSizedOperandSegments, HasParent<"CallSubquery">]> {
  let summary = "Name the row tag and the columns a db.call_subquery's body contributes";

  let description = [{
    Terminates a db.call_subquery's body region. `tag` is the row tag as the body
    left it, present exactly when the body takes one, and `columns` are the columns
    the body contributes: the input columns as the body left them when the op
    carries its scope, then the columns the body's RETURN publishes. A unit body
    yields no column.
  }];

  let arguments = (ins Optional<ColumnRowTags>:$tag, Variadic<Column>:$columns);

  let assemblyFormat = [{
    ($tag^ `,`)? `{` $columns `}` attr-dict `:` `{` qualified(type($columns)) `}`
  }];
}

/**
* ExistsSubquery
*/
def ExistsSubquery : TuringOp<"exists_subquery", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns", "getRegionCarrySet"]>]> {
  let summary = "Answer, per row in flight, whether a body produces a row of its own for it";

  let description = [{
    Models Cypher's `EXISTS { ... }`: a semi-join of the rows in flight against a
    read-only body. The result is one boolean per input row - true exactly when the
    body produced at least one row for it - so the op changes no row count, which is
    what lets it stand in an expression.

    `input_columns` are every named column the query holds at this point, and the body
    region takes one argument per input column - the body reads the scope around it
    through them, EXISTS being correlated - plus a trailing `!db.column<ui64>` row tag:
    the position of each input row, carried through the body the way any other column
    is, so the rows that produced one are known once the body is walked. An EXISTS over
    no input column joins onto the single empty row Cypher starts from and takes no tag.

      %b = db.exists_subquery(%p) carries_scope ({
        ^bb0(%pIn: !db.column<!storage.node_id>, %tag: !db.column<ui64>):
          %s, %e, %et, %k, %tag2 = db.get_out_edges(%pIn, {%tag}) : ...
          db.exists_yield %tag2, {}
      }) : (!db.column<!storage.node_id>) -> !db.column<!storage.bool>

    `carries_scope` says the body's own dataflow keeps every input row paired with what
    it made of it, so it runs over a whole chunk of input rows at once and the tag comes
    back naming the rows that survived. A body that aggregates, sorts, cuts or dedups
    cannot: it is run one input row at a time instead, and the row is marked when the
    body yielded anything at all for it.

    The db.exists_yield names the tag as the body left it, which is all a tagged body is
    answered from. A body with no tag names the columns it holds instead, which carry the
    row count it answers from.

    Reading the graph is deterministic and an EXISTS body writes nothing, so it is Pure.
  }];

  let arguments = (ins Variadic<Column>:$input_columns, UnitAttr:$carries_scope);

  let results = (outs ColumnBools:$result);

  // Exactly one block, ending in a db.exists_yield.
  let regions = (region SizedRegion<1>:$body);

  let assemblyFormat = [{
    `(` $input_columns `)` (`carries_scope` $carries_scope^)? $body attr-dict
    `:` functional-type($input_columns, results)
  }];

  let hasVerifier = 1;
}

/**
* ExistsYield
*/
def ExistsYield : TuringOp<"exists_yield", [Pure, Terminator, AttrSizedOperandSegments, HasParent<"ExistsSubquery">]> {
  let summary = "Name the row tag and the columns a db.exists_subquery's body holds";

  let description = [{
    Terminates a db.exists_subquery's body region. `tag` is the row tag as the body left
    it - the input row each surviving row came from, absent when the op has no input
    column. A body with no tag names in `columns` the columns it holds there, which it
    reads its row count off; a tagged body names none.
  }];

  let arguments = (ins Optional<ColumnRowTags>:$tag, Variadic<Column>:$columns);

  // The tag's type is buildable and so left out; only the held column types are spelled,
  // in braces as db.optional_yield spells the ones its pattern contributes.
  let assemblyFormat = [{
    ($tag^ `,`)? `{` $columns `}` attr-dict (`:` `{` qualified(type($columns))^ `}`)?
  }];
}

/**
* CountSubquery
*/
def CountSubquery : TuringOp<"count_subquery", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns", "getRegionCarrySet"]>]> {
  let summary = "Answer, per row in flight, how many rows a body produces for it";

  let description = [{
    Models Cypher's `COUNT { ... }`. The result is one count per input row - the number
    of rows the body produced for it, 0 where it produced none - so the op changes no row
    count, which is what lets it stand in an expression.

    Its operands, body and paths are db.exists_subquery's: the body reads the scope around
    it through one block argument per input column, and a body carrying the scope takes a
    trailing `!db.column<ui64>` row tag and is run over a whole chunk of input rows at
    once, each row it yields counted toward the input row its tag names. A body that
    aggregates, sorts, cuts, dedups or is a UNION is run one input row at a time instead,
    and the rows it yields are counted toward that row.

      %n = db.count_subquery(%p) carries_scope ({
        ^bb0(%pIn: !db.column<!storage.node_id>, %tag: !db.column<ui64>):
          %s, %e, %et, %k, %tag2 = db.get_out_edges(%pIn, {%tag}) : ...
          db.count_subquery_yield %tag2, {}
      }) : (!db.column<!storage.node_id>) -> !db.column<ui64>

    Reading the graph is deterministic and a COUNT body writes nothing, so it is Pure.
  }];

  let arguments = (ins Variadic<Column>:$input_columns, UnitAttr:$carries_scope);

  let results = (outs ColumnCounts:$result);

  // Exactly one block, ending in a db.count_subquery_yield.
  let regions = (region SizedRegion<1>:$body);

  let assemblyFormat = [{
    `(` $input_columns `)` (`carries_scope` $carries_scope^)? $body attr-dict
    `:` functional-type($input_columns, results)
  }];

  let hasVerifier = 1;
}

/**
* CountSubqueryYield
*/
def CountSubqueryYield : TuringOp<"count_subquery_yield", [Pure, Terminator, AttrSizedOperandSegments, HasParent<"CountSubquery">]> {
  let summary = "Name the row tag and the columns a db.count_subquery's body holds";

  let description = [{
    Terminates a db.count_subquery's body region. `tag` is the row tag as the body left
    it, absent when the body takes none. A body with no tag names in `columns` the columns
    it holds there, which it counts its rows off; a tagged body names none.
  }];

  let arguments = (ins Optional<ColumnRowTags>:$tag, Variadic<Column>:$columns);

  let assemblyFormat = [{
    ($tag^ `,`)? `{` $columns `}` attr-dict (`:` `{` qualified(type($columns))^ `}`)?
  }];
}

/**
* CollectSubquery
*/
def CollectSubquery : TuringOp<"collect_subquery", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns", "getRegionCarrySet"]>]> {
  let summary = "Build, per row in flight, the list of what a body returns for it";

  let description = [{
    Models Cypher's `COLLECT { ... }`. The result is one list per input row - the values
    of the one column the body returns for it, in the order the body produced them, and
    the empty list where it produced none - so the op changes no row count, which is what
    lets it stand in an expression.

    Its operands, body and paths are db.count_subquery's: the body reads the scope around
    it through one block argument per input column, and a body carrying the scope takes a
    trailing `!db.column<ui64>` row tag and is run over a whole chunk of input rows at
    once, each value it yields going into the list of the input row its tag names. A body
    that aggregates, sorts, cuts, dedups or is a UNION is run one input row at a time
    instead, and every value it yields goes into that row's list.

      %names = db.collect_subquery(%p) carries_scope ({
        ^bb0(%pIn: !db.column<!storage.node_id>, %tag: !db.column<ui64>):
          %s, %e, %et, %k, %tag2 = db.get_out_edges(%pIn, {%tag}) : ...
          %name = db.get_node_properties(%k) {property = "name"} : ...
          db.collect_subquery_yield tag %tag2, %name, {} : !db.column<none>
      }) : (!db.column<!storage.node_id>) -> !db.column<none>

    The result's type is the body's to decide and is resolved during lowering, as
    db.pattern_comprehension's is: the list is of whatever the body yields. A null the
    body returns is an element of the list, as it is in Neo4j.

    Reading the graph is deterministic and a COLLECT body writes nothing, so it is Pure.
  }];

  let arguments = (ins Variadic<Column>:$input_columns, UnitAttr:$carries_scope);

  let results = (outs Column:$result);

  // Exactly one block, ending in a db.collect_subquery_yield.
  let regions = (region SizedRegion<1>:$body);

  let assemblyFormat = [{
    `(` $input_columns `)` (`carries_scope` $carries_scope^)? $body attr-dict
    `:` functional-type($input_columns, results)
  }];

  let hasVerifier = 1;
}

/**
* CollectSubqueryYield
*/
def CollectSubqueryYield : TuringOp<"collect_subquery_yield", [Pure, Terminator, AttrSizedOperandSegments, HasParent<"CollectSubquery">]> {
  let summary = "Name the row tag, the value and the columns a db.collect_subquery's body holds";

  let description = [{
    Terminates a db.collect_subquery's body region. `tag` is the row tag as the body left
    it, absent when the body takes none, and `value` is the one column the body returns,
    which each row it produced contributes to a list. A body with no tag names in
    `columns` the columns it holds there, which its row count is read off; a tagged body
    names none.
  }];

  let arguments = (ins Optional<ColumnRowTags>:$tag, Column:$value, Variadic<Column>:$columns);

  // The tag and the value are both operands, so the tag is spelled behind a keyword that
  // tells the parser which of the two it is reading
  let assemblyFormat = [{
    (`tag` $tag^ `,`)? $value `,` `{` $columns `}` attr-dict `:` qualified(type($value))
    (`,` `{` qualified(type($columns))^ `}`)?
  }];
}

#endif //TURINGDB_OPS
