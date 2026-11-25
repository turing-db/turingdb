#ifndef TURINGDB_SUBQUERY_OPS
#define TURINGDB_SUBQUERY_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"

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

#endif // TURINGDB_SUBQUERY_OPS
