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
* CrossProduct
*/
def CrossProduct : TuringOp<"cross_product", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getRowSource", "getYieldedResultOffset"]>]> {
  let summary = "Cross every row of one column dataflow with every row of another";

  let description = [{
    Models a Cypher disconnected pattern, e.g. `MATCH (a)-->(b), (c)-->(d)`: two
    column dataflows that share no variable, whose result is the cartesian
    product of their rows. Each side is a self-contained db dataflow held in its
    own region - `leftFactor` and `rightFactor` - with no operands and no block
    arguments, so the two factors share no SSA value.

      %a, %b = db.cross_product factor {
        %x = db.scan_nodes() : !db.column<!storage.node_id>
        db.yield %x : !db.column<!storage.node_id>
      } factor {
        %y = db.scan_nodes() : !db.column<!storage.node_id>
        db.yield %y : !db.column<!storage.node_id>
      }

    Because the factors are disconnected, which columns end up in the result
    cannot be read off a data dependency (unlike get_out_edges' carry set within
    a connected traversal) - it must be stated. Each region ends with a db.yield
    naming exactly the columns that factor contributes, and the op's results are
    the left factor's yielded columns followed by the right factor's. Each factor
    must yield at least one column: a side's row count is read from its first
    yielded column, so a factor that surfaces none cannot be sized and the
    verifier rejects an empty db.yield.

    The result types are not spelled after the regions; the parser recovers them
    from the two yields and the verifier checks they line up. Reading the graph
    is deterministic, so the op is Pure.
  }];

  // The results are the concatenation of the two factors' yielded columns; the
  // count and types are recovered from the yields, never spelled out.
  let results = (outs Variadic<Column>:$results);

  // Each factor is exactly one block: a self-contained dataflow ending in a
  // db.yield, with no operands and no block arguments.
  let regions = (region SizedRegion<1>:$leftFactor, SizedRegion<1>:$rightFactor);

  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // The only builder takes the result types and creates the two empty factor
  // blocks; the caller fills each region and terminates it with a db.yield
  // whose operands match the result types contributed by that factor.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::TypeRange":$resultTypes)>
  ];
}

/**
* HashJoin
*/
def HashJoin : TuringOp<"hash_join", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getRowSource", "getYieldedResultOffset"]>]> {
  let summary = "Cross two column dataflows, keeping the rows their key columns agree on";

  let description = [{
    The fused form of a db.cross_product whose product one db.filter cuts by a
    single db.eq between a column of each factor - an inner equi-join. It holds
    each side the way the product does, in its own region ending in a db.yield,
    and its results are the left factor's yielded columns followed by the right
    factor's.

      %a, %ka, %b, %kb = db.hash_join factor {
        %x = db.scan_nodes() : !db.column<!storage.node_id>
        %k = db.get_node_properties(%x, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
        db.yield %x, %k : !db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>
      } factor {
        %y = db.scan_nodes() : !db.column<!storage.node_id>
        %k = db.get_node_properties(%y, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
        db.yield %y, %k : !db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>
      } on 1, 1

    `on` names the key column of each side as an index into that factor's
    db.yield - the left factor's first, the right factor's second. A result row
    pairs a left row with a right row whose keys `=` holds equal, so the two keys
    need not share a type: 32 joins 32.0. A row whose key is null joins with
    nothing, which is what the equality it replaces did: `=` against a null gives
    null and the filter keeps only the true rows.

    The right factor is the built side and the left the probed one, so a left
    row's matches come out together and in right-factor order, however either
    side was chunked. That is not the order the product and its filter emitted
    the surviving rows in: the product re-runs its right factor once per chunk
    of the left, so a right side spanning several chunks interleaves them. Row
    order is what an ORDER BY is for, so the fusion is free to change it - but
    a LIMIT with no ORDER BY over a join can keep other rows than the same
    LIMIT over the product.

    As on db.cross_product the result types are not spelled after the regions;
    the parser recovers them from the two yields and the verifier checks they line
    up. Reading the graph is deterministic, so the op is Pure.
  }];

  // Which column of each factor's yield carries the join key: the left factor's,
  // then the right factor's.
  let arguments = (ins UI64Attr:$leftKey, UI64Attr:$rightKey);

  // The results are the concatenation of the two factors' yielded columns; the
  // count and types are recovered from the yields, never spelled out.
  let results = (outs Variadic<Column>:$results);

  // Each factor is exactly one block: a self-contained dataflow ending in a
  // db.yield, with no operands and no block arguments.
  let regions = (region SizedRegion<1>:$leftFactor, SizedRegion<1>:$rightFactor);

  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // The only builder takes the result types and the two key indices and creates
  // the two empty factor blocks; the caller fills each region and terminates it
  // with a db.yield whose operands match the result types contributed by that
  // factor.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::TypeRange":$resultTypes, "uint64_t":$leftKey, "uint64_t":$rightKey)>
  ];
}

/**
* Yield
*/
def Yield : TuringOp<"yield", [Pure, Terminator, ParentOneOf<["CrossProduct", "HashJoin", "Union", "Conditional", "ExplorePaths"]>]> {
  let summary = "Name the columns a region contributes to the result of the op holding it";

  let description = [{
    Terminates a factor region of a db.cross_product or a db.hash_join, naming
    the columns that factor contributes. Either op's results are the left
    factor's yielded columns followed by the right factor's. A factor must yield
    at least one column - the holder's verifier rejects an empty yield, since a
    side's row count is read from its first yielded column.

    It also terminates each branch of a db.union that has results and of a
    db.conditional, naming the rows that branch adds to them. It terminates each
    condition of a db.conditional and the hop region of a db.explore_paths too, yielding
    one boolean column: whether the row takes the branch, or which candidates of a hop
    pass its predicate.
  }];

  let arguments = (ins Variadic<Column>:$columns);

  // The yielded column types vary from query to query and cannot be built from
  // a recipe, so - when present - they are spelled after the colon, as in
  // db.output. The optional group, anchored on the operands, keeps the syntax
  // total: an empty yield (`db.yield`) still parses, then the holder's verifier
  // rejects it with a clear db-level diagnostic.
  let assemblyFormat = "( $columns^ `:` qualified(type($columns)) )? attr-dict";
}

/**
* Output
*/
def Output : TuringOp<"output"> {
  let summary = "Project a set of columns as the query result";
  let description = [{
      Sends a number of columns to output.

      db.output(%a, %b, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>

    Variadic<Column> accepts any number of columns of any value. The op has no
    results and is deliberately not Pure: emitting the result is its whole
    purpose, so it must survive dead-code elimination.

    An optional `names` array gives the result columns their query-level names,
    one per column: the alias a RETURN item was given through AS, or the item
    spelled as it was written when it has none. A hand-written program that
    names nothing leaves the array off, and the result columns are unnamed.

      db.output(%a, %b) names ["person", "n.age"] : !db.column<!storage.node_id>, !db.column<i64>
  }];
  let arguments = (ins Variadic<Column>:$columns,
                       OptionalAttr<StrArrayAttr>:$column_names);

  // The column types vary from query to query and cannot be built from a
  // recipe, so they are spelled after the colon, the same as nl.output.
  // qualified(...) forces the leading !db. on every printed type. The names,
  // when present, print after a `names` keyword, which anchors the optional
  // group for the declarative parser.
  let assemblyFormat = [{
    `(` $columns `)` (`names` $column_names^)? attr-dict `:` qualified(type($columns))
  }];

  // A hand-written program names nothing, so it gets a builder taking the columns alone.
  let builders = [
      OpBuilder<(ins "::mlir::ValueRange":$columns)>
  ];

  // One name per column or none at all: a shorter array would name the wrong
  // column, and a longer one names a column that is not emitted.
  let hasVerifier = 1;
}

/**
* Limit
*/
def Limit : TuringOp<"limit", [Pure, AttrSizedOperandSegments, KeepsRowsByPosition, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getCarrySetMutable"]>]> {
  let summary = "Take the first N rows of a column dataflow";
  let description = [{
    The declarative counterpart of a Cypher RETURN ... LIMIT N. Caps the result
    to the first N rows of the columns flowing into db.output, passing them
    through unchanged:

      %a  = db.scan_nodes() : !db.column<!storage.node_id>
      %la = db.limit(%a) count 3 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
      db.output(%la) : !db.column<!storage.node_id>

    The count is either `count`, the one the query spells, or `bound_count`, an
    integer column holding one value for the whole run, which the limit reads when
    its loop starts: a parameter, or what ops compute from parameters and literals.
    An op that raises computing it fails the query, whatever rows it reads. `LIMIT $n`
    limits

      %la = db.limit(%a) count %n : !db.column<i64> : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>

    Exactly one of the two is given, and the program does not depend on the count
    bound. A bound count that is negative or null fails the query when the loop
    starts.

    It lowers to a hoisted nl.limit handle, an nl.limit_update in the innermost
    loop body, a `limit` operand on every nl.for in the nest, and a `limit`
    operand on nl.output. Reading a prefix of the graph is deterministic, so the
    op is Pure.
  }];

  // The columns flow through unchanged; count is unsigned - a row budget is
  // never negative, so a negative literal fails to parse rather than reaching
  // the verifier. The bound count comes first, so the carry set is the trailing
  // operands.
  let arguments = (ins Optional<Column>:$bound_count, Variadic<Column>:$columns, OptionalAttr<UI64Attr>:$count);
  let results   = (outs Variadic<Column>:$results);

  // One result per input column, same types; functional-type prints the
  // `(columnTypes) -> resultTypes` form and the count prints ahead of the colon,
  // elided from attr-dict.
  let assemblyFormat = [{
    `(` $columns `)` `count` custom<BoundCount>($count, $bound_count, type($bound_count)) attr-dict `:` functional-type($columns, results)
  }];

  let builders = [
    OpBuilder<(ins "::mlir::TypeRange":$results, "::mlir::ValueRange":$columns, "uint64_t":$count), [{
      build($_builder, $_state, results, ::mlir::Value(), columns, $_builder.getIntegerAttr($_builder.getIntegerType(64, /*isSigned=*/false), count));
    }]>,
    OpBuilder<(ins "::mlir::TypeRange":$results, "::mlir::ValueRange":$columns, "::mlir::Value":$boundCount), [{
      build($_builder, $_state, results, boundCount, columns, ::mlir::IntegerAttr());
    }]>
  ];

  // results arity and types must equal the columns, and the count is given once.
  let hasVerifier = 1;
}

/**
* Skip
*/
def Skip : TuringOp<"skip", [Pure, AttrSizedOperandSegments, KeepsRowsByPosition, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getCarrySetMutable"]>]> {
  let summary = "Drop the first M rows of a column dataflow";
  let description = [{
    The declarative counterpart of a Cypher RETURN ... SKIP M. Drops the first M
    rows of the columns flowing through it, passing the rest through unchanged:

      %a  = db.scan_nodes() : !db.column<!storage.node_id>
      %sa = db.skip(%a) count 3 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
      db.output(%sa) : !db.column<!storage.node_id>

    The count is either `count` or `bound_count`, as db.limit's is: `SKIP $n` skips

      %sa = db.skip(%a) count %n : !db.column<i64> : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  }];

  // The columns flow through unchanged; count is unsigned - a count of rows to
  // drop is never negative, so a negative literal fails to parse rather than
  // reaching the verifier. The bound count comes first, so the carry set is the
  // trailing operands.
  let arguments = (ins Optional<Column>:$bound_count, Variadic<Column>:$columns, OptionalAttr<UI64Attr>:$count);
  let results   = (outs Variadic<Column>:$results);

  // One result per input column, same types; functional-type prints the
  // `(columnTypes) -> resultTypes` form and the count prints ahead of the colon,
  // elided from attr-dict.
  let assemblyFormat = [{
    `(` $columns `)` `count` custom<BoundCount>($count, $bound_count, type($bound_count)) attr-dict `:` functional-type($columns, results)
  }];

  let builders = [
    OpBuilder<(ins "::mlir::TypeRange":$results, "::mlir::ValueRange":$columns, "uint64_t":$count), [{
      build($_builder, $_state, results, ::mlir::Value(), columns, $_builder.getIntegerAttr($_builder.getIntegerType(64, /*isSigned=*/false), count));
    }]>,
    OpBuilder<(ins "::mlir::TypeRange":$results, "::mlir::ValueRange":$columns, "::mlir::Value":$boundCount), [{
      build($_builder, $_state, results, boundCount, columns, ::mlir::IntegerAttr());
    }]>
  ];

  // results arity and types must equal the columns, and the count is given once.
  let hasVerifier = 1;
}

/**
* Sort
*/
def Sort : TuringOp<"sort", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet"]>]> {
  let summary = "Reorder the rows of a column set by one or more key columns";
  let description = [{
    The declarative counterpart of a Cypher RETURN ... ORDER BY. Takes the whole
    set of columns flowing into db.output and emits them with their rows reordered
    so that the key columns are sorted; every column moves as one row, so the
    projection stays row-aligned:

      %a     = db.scan_nodes() : !db.column<!storage.node_id>
      %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
      %sa, %sscore = db.sort(%a, %score) keys [1] ascending [true]
                       : (!db.column<!storage.node_id>, !db.column<!storage.nullable<i64>>) -> (!db.column<!storage.node_id>, !db.column<!storage.nullable<i64>>)
      db.output(%sa, %sscore) : !db.column<!storage.node_id>, !db.column<!storage.nullable<i64>>

    A row is sorted across several columns: `keys` is the list of column indices
    to sort by, most significant first (so `keys [1, 0]` sorts by column 1, ties
    broken by column 0), and `ascending` gives the direction of each key. The
    columns pass through unchanged in count and type - only the row order changes.

    Sort is a pipeline breaker: it must see every row before it can emit any, so
    it lowers to an nl.sort_buffer accumulator hoisted above the loops, an
    nl.sort_collect in the producing loop body that appends each chunk, and an
    nl.sort source iterator after the loop whose nl.for emits the sorted rows.
    Reordering a deterministic read of the graph is itself deterministic, so the
    op is Pure.
  }];

  // The columns flow through unchanged. keyColumns are indices into $columns,
  // most significant first; keyAscending gives one direction per key (true =
  // ascending). The two arrays are parallel and the verifier checks they line up.
  let arguments = (ins Variadic<Column>:$columns,
                       DenseI64ArrayAttr:$keyColumns,
                       DenseBoolArrayAttr:$keyAscending);
  let results   = (outs Variadic<Column>:$results);

  // One result per input column, same types; functional-type prints the
  // `(operandTypes) -> resultTypes` form. The two key arrays print after their
  // keywords (`keys array<i64: ...>`, `ascending array<i1: ...>`) ahead of the
  // colon, elided from attr-dict because the format already names them.
  let assemblyFormat = [{
    `(` $columns `)` `keys` $keyColumns `ascending` $keyAscending attr-dict `:`
    functional-type(operands, results)
  }];

  // results arity and types must equal the columns, and the key arrays must be
  // parallel and reference columns in range.
  let hasVerifier = 1;
}

/**
* RemoveDuplicates
*/
// NOT Pure: it records each row it emits in a seen-set, so it must run where it sits and
// must not be hoisted, CSE'd or eliminated (like nl.distinct_filter, which it lowers to).
def RemoveDuplicates : TuringOp<"remove_duplicates", [AttrSizedOperandSegments, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet", "getCarrySetMutable"]>]> {
  let summary = "Emit each distinct row of a column set once";
  let description = [{
    The declarative counterpart of a Cypher RETURN DISTINCT / WITH DISTINCT. Takes
    the whole set of columns flowing through it and emits only the first occurrence
    of each distinct row, dropping later duplicates. A row's identity is the tuple
    across all its columns - so the whole projection is the dedup key - and two
    nulls count as equal, as Cypher DISTINCT dedups nulls together. The columns
    pass through unchanged in count and type; only duplicate rows are removed:

      %a  = db.scan_nodes() : !db.column<!storage.node_id>
      %da = db.remove_duplicates(%a) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
      db.output(%da) : !db.column<!storage.node_id>

    Unlike db.sort it is not a pipeline breaker: it emits a row the moment it first
    sees it, keeping only a growing seen-set. It lowers to a hoisted nl.distinct
    seen-set handle and an nl.distinct_filter in the producing loop body that emits
    each step's not-yet-seen rows as fresh chunks - consumed by db.output when the
    query ends here, or by a downstream traversal when a WITH DISTINCT feeds a
    further MATCH. Because it streams and shrinks each chunk, a following db.limit
    charges the deduped survivor count and bounds the producing loop through the
    ordinary early-exit, so DISTINCT ... LIMIT k needs no special fusion - the scan
    stops once k distinct rows are found.
  }];

  // The columns flow through unchanged; every column is part of the dedup key, so
  // - unlike db.sort's keys - no key attribute is needed. The optional set is the
  // seen-set this dedup shares with its siblings; absent, it keeps one of its own.
  let arguments = (ins Variadic<Column>:$columns, Optional<DistinctSetType>:$set);
  let results   = (outs Variadic<Column>:$results);

  // One result per input column, same types; functional-type prints the
  // `(operandTypes) -> resultTypes` form, the same as db.limit and db.sort. The set
  // prints after an `against` keyword, ahead of the colon, so the functional type
  // stays the columns alone.
  let assemblyFormat = [{
    `(` $columns `)` (`against` $set^)? attr-dict `:` functional-type($columns, $results)
  }];

  // results arity and types must equal the columns (the shared pass-through check).
  let hasVerifier = 1;
}

/**
* DistinctSet
*/
// NOT Pure: two seen-sets must never be CSE'd into one, and this op has no operands to
// tell one from another (like nl.distinct, which it lowers to).
def DistinctSet : TuringOp<"distinct_set", []> {
  let summary = "Open a dedup key set for several db.remove_duplicates to share";
  let description = [{
    A UNION dedups its whole result, but each of its branches reaches that result
    through a db.output of its own, so no single db.remove_duplicates sees every row.
    This produces the set they all record their rows in, making the dedup one:

      %s = db.distinct_set : !db.distinct_set
      db.union {
        ...
        %da = db.remove_duplicates(%a) against %s : (!db.column<none>) -> !db.column<none>
        db.output(%da) names ["name"] : !db.column<none>
      }, {
        ...
      }

    It lowers to the single nl.distinct handle every branch's nl.distinct_filter reads.
    A db.remove_duplicates naming no set is unaffected: it keeps a set of its own.
  }];

  let results = (outs DistinctSetType:$set);
  let assemblyFormat = "attr-dict `:` qualified(type($set))";
}

/**
* Materialize
*/
def Materialize : TuringOp<"materialize", [Pure]> {
  let summary = "Hold every row in flight for several dataflows to read in turn";
  let description = [{
    The table a NEXT hands a UNION segment, every branch of which reads all of it. The
    columns in flight are held once and each branch walks them with a db.scan_materialized
    of its own, as a common table expression is scanned once per reference:

      %t = db.materialize(%name, %age) : (!db.column<none>, !db.column<none>) -> !db.materialized
      %n = db.union {
        %name1, %age1 = db.scan_materialized(%t) : (!db.materialized) -> (!db.column<none>, !db.column<none>)
        ...
        db.yield %c : !db.column<ui64>
      }, {
        %name2, %age2 = db.scan_materialized(%t) : (!db.materialized) -> (!db.column<none>, !db.column<none>)
        ...
        db.yield %l : !db.column<none>
      } : !db.column<none>

    A pipeline breaker: a scan starts once the dataflow feeding the op has run to its end.
    The columns are row-aligned, the rows of one relation, and no constant is among them:
    a constant stands for every row, and a branch reads it where it is bound.
  }];

  let arguments = (ins Variadic<Column>:$columns);
  let results   = (outs MaterializedType:$rows);

  let assemblyFormat = [{
    `(` $columns `)` attr-dict `:` functional-type($columns, $rows)
  }];

  let hasVerifier = 1;
}

/**
* ScanMaterialized
*/
def ScanMaterialized : TuringOp<"scan_materialized", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns"]>]> {
  let summary = "Walk every row a db.materialize holds";
  let description = [{
    A source like a scan: it opens a loop over the rows `rows` holds, in the order they
    were held, one result per column the db.materialize took, of the same type.
  }];

  let arguments = (ins MaterializedType:$rows);
  let results   = (outs Variadic<Column>:$columns);

  let assemblyFormat = [{
    `(` $rows `)` attr-dict `:` functional-type($rows, $columns)
  }];

  let hasVerifier = 1;
}

/**
* Union
*/
def Union : TuringOp<"union", [NoTerminator, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["collectCarriedColumns"]>]> {
  let summary = "Emit the results of several query bodies as one result table";
  let description = [{
    The declarative counterpart of Cypher's UNION and UNION ALL. Holds one region per
    branch, each a self-contained db query body ending in its own db.output, and emits
    their rows one branch after another:

      db.union {
        %a = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
        %n = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
        db.output(%n) names ["name"] : !db.column<!storage.nullable<!storage.string>>
      }, {
        %c = db.scan_nodes_by_label(["City"]) : !db.column<!storage.node_id>
        %m = db.get_node_properties(%c, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
        db.output(%m) names ["name"] : !db.column<!storage.nullable<!storage.string>>
      }

    The branches share no SSA value and read none from outside, bar the db.distinct_set
    a deduping union hands each of them. Every branch outputs the same number of columns
    under the same names - one result table - so the op needs no result of its own and is
    the whole program, as a lone db.output is for a query without a union.

    UNION (as against UNION ALL) is not held here: a deduping union is spelled as a
    db.distinct_set above the op and a db.remove_duplicates against it in each branch it
    covers, so the dedup is in the dataflow it applies to rather than in a flag on the
    holder. It lowers to one loop nest per branch, run in order, in the same nl function -
    the concatenation is the sink seeing each nest's rows in turn. Not Pure: its branches
    emit the result, so it must survive dead-code elimination as db.output does.

    The body of a db.call_subquery is a UNION when the CALL's is, and so is a UNION
    segment of a NEXT chain. The rows the branches produce then feed what follows rather
    than the result table, so the op has one result per column and each branch ends in a
    db.yield of its columns. The branches of a CALL body read the body's arguments, the
    columns of the row the body runs for; those of a segment read the table in flight
    through a db.scan_materialized each:

      %z = db.union {
        %s, %e, %et, %i = db.get_out_edges(%pIn, {}) : ...
        db.yield %i : !db.column<!storage.node_id>
      }, {
        %s2, %e2, %et2, %k = db.get_in_edges(%pIn, {}) : ...
        db.yield %k : !db.column<!storage.node_id>
      } : !db.column<!storage.node_id>
  }];

  let results = (outs Variadic<Column>:$results);

  // One region per branch, each exactly one block. The blocks have no arguments: a
  // branch reads nothing the branch before it bound.
  let regions = (region VariadicRegion<SizedRegion<1>>:$branches);

  let hasCustomAssemblyFormat = 1;
  let hasVerifier = 1;

  // Both builders take the branch count and create that many empty blocks; the caller
  // fills each region and ends it with a db.output, or with a db.yield when the op has
  // results.
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "size_t":$branchCount)>,
      OpBuilder<(ins "::mlir::TypeRange":$resultTypes, "size_t":$branchCount)>
  ];
}

/**
* Conditional
*/
def Conditional : TuringOp<"conditional", []> {
  let summary = "Run the first branch whose condition is true, or the default one";
  let description = [{
    The declarative counterpart of Cypher's WHEN ... THEN ... ELSE. The regions alternate
    condition and branch, one pair per WHEN, and end with the ELSE branch when there is
    one. Branch i runs when condition i is true and no condition before it is, and the
    ELSE when none is. A null condition is not true. A condition is computed only when no
    branch before it is taken, and a branch that does not run computes nothing, an
    aggregate in it included.

    It sits in the body of a db.call_subquery, which runs it once per input row, or at
    the top of a standalone WHEN, where nothing is in flight. Either way a condition holds
    one row. The regions read the scope around the op the way the branches of a db.union
    in a CALL body do. A condition ends in a db.yield of one boolean column, and each
    branch in a db.yield of the same number of columns, which are the op's results;
    branches that only write yield none:

      %z = db.conditional ({
        %c = db.get_node_properties(%pIn, "isFrench") : ...
        db.yield %c : !db.column<none>
      }, {
        %n = db.get_node_properties(%pIn, "name") : ...
        db.yield %n : !db.column<none>
      }, {
        %c = db.constant("other") : !db.column<!storage.string>
        db.yield %c : !db.column<!storage.string>
      }) : !db.column<none>

    Not Pure: its branches may write to the graph.
  }];

  let results = (outs Variadic<Column>:$results);

  let regions = (region VariadicRegion<SizedRegion<1>>:$branches);

  let assemblyFormat = [{
    `(` $branches `)` attr-dict (`:` type($results)^)?
  }];

  let hasVerifier = 1;

  // Creates one empty block per region; the caller fills each and ends it with a db.yield
  let skipDefaultBuilders = 1;
  let builders = [
      OpBuilder<(ins "::mlir::TypeRange":$resultTypes, "size_t":$regionCount)>
  ];
}

/**
* Count
*/
def Count : TuringOp<"count", [Pure, AggregateOpInterface]> {
  let summary = "Count the non-null rows of a column as a single-row result";
  let description = [{
    The declarative counterpart of a Cypher RETURN count(x). Consumes the whole
    dataflow of one column and produces a single-row column holding how many of its
    rows are non-null - matching Cypher, where count(x) counts the rows in which x
    is not null. A never-null column (node IDs off a scan) counts every row, so
    count(*) is expressed by counting such a column:

      %a = db.scan_nodes() : !db.column<!storage.node_id>
      %n = db.count(%a) : (!db.column<!storage.node_id>) -> !db.column<ui64>
      db.output(%n) : !db.column<ui64>

    `rows` makes it Cypher's count(*): the tally charges every row, whatever the input
    column holds, so the wildcard can be anchored on whichever column of the query part
    carries its rows - a nullable property column included, which a null-skipping tally
    would under-count:

      %n = db.count(%age) rows : (!db.column<none>) -> !db.column<ui64>

    `distinct` makes it Cypher's count(DISTINCT x): the tally charges each distinct
    non-null value once, however many rows carry it. It stays one op rather than
    two because the reduction is the same tally over a deduplicated column - it
    lowers to the DISTINCT seen-set (nl.distinct / nl.distinct_filter) feeding the
    ordinary nl.count_update, so nulls are dropped by the count as usual:

      %n = db.count(%a) distinct : (!db.column<!storage.node_id>) -> !db.column<ui64>
  }];

  // One column in, one integer column out. The input and result types are
  // unrelated (an integer count of any column), so - unlike the pass-through ops -
  // there is nothing to cross-check and no verifier. distinct and rows are unit
  // attributes: their presence is the flag, so a plain db.count is two absent ones.
  let arguments = (ins Column:$input, UnitAttr:$distinct, UnitAttr:$rows);
  let results   = (outs Column:$result);

  // One operand and one result, so functional-type(operands, results) prints the
  // `(!db.column<...>) -> !db.column<...>` form, the same as db.get_node_properties.
  // The flags print as their keywords ahead of the colon when set, so a plain count
  // keeps the form it had before they existed.
  let assemblyFormat = [{
    `(` $input `)` (`distinct` $distinct^)? (`rows` $rows^)? attr-dict `:` functional-type(operands, results)
  }];
}

/**
* CountScanRows
*/
def CountScanRows : TuringOp<"count_scan_rows", [Pure, AggregateOpInterface]> {
  let summary = "Count the rows of whole node scans from the graph's node counts";

  let description = [{
    What a db.count collapses to when the rows it tallies are whole node scans and
    nothing else. `MATCH (a:Reaction) RETURN count(*)` asks how many Reaction nodes
    there are, not which ones, and the graph already holds that number per label -
    so the answer is a metadata read rather than a walk.

    `labels` lists one label conjunction per scan, in factor order, and the result
    is the product of their node counts - the row count of the cartesian product
    those scans form:

      %n = db.count_scan_rows([["Reaction"]]) : !db.column<none>
      %m = db.count_scan_rows([["Reaction"], ["Pathway"]]) : !db.column<none>

    An empty conjunction is an unlabelled scan and counts every node, so
    `MATCH (a) RETURN count(*)` is db.count_scan_rows([[]]). The list itself must
    not be empty: with no scan listed there is nothing to count. A label absent from
    the schema matches no node, so its factor counts 0 and the whole product is 0 -
    the same unsatisfiable conjunction db.scan_nodes_by_label yields no row for.

    `property`, when given, narrows one scan to the nodes holding that property -
    Cypher's count(a.name), which counts the rows in which a.name is not null, so the
    answer is how many of that scan's nodes hold it rather than how many there are.
    That too the graph knows without reading a value: it indexes each property by the
    label set of the nodes holding it.

      %n = db.count_scan_rows([["Person"]]) property "name" : !db.column<none>

    `propertyScan` is which of the listed scans it is read from, counted from 0 and
    defaulting to the first. `MATCH (a:Reaction), (b:Pathway) RETURN count(b.stId)`
    reads it from the second, so the tally is every Reaction crossed with the Pathways
    that hold stId:

      %m = db.count_scan_rows([["Reaction"], ["Pathway"]]) property "stId" of scan 1
             : !db.column<none>

    A count reads one column, so at most one scan is ever narrowed; the others
    contribute their whole node count.

    The result is the single-row unsigned tally db.count produces, typed as loosely
    as db.count types its own so that replacing one leaves its consumers untouched.
    Reading the counts is a deterministic read, so the op is Pure.
  }];

  // `labels` is one label conjunction per scan, `property` the name whose holders one
  // of them is narrowed to and `propertyScan` which one. Being attributes and not SSA
  // values, they are absent from `operands`; lowering forwards them as-is.
  let arguments = (ins LabelRefArrayArrayAttr:$labels,
                       OptionalAttr<StrAttr>:$property,
                       OptionalAttr<UI64Attr>:$propertyScan);
  let results   = (outs Column:$result);
  let hasVerifier = 1;

  // The conjunctions print inside the parens as `[["Reaction"], ["Pathway"]]`, ahead
  // of the colon, the same place db.scan_nodes_by_label prints its labels; the
  // property and the scan it is read from follow their keywords when set, the way
  // db.count prints its flags; the result type follows, qualified, as db.count prints
  // its own.
  let assemblyFormat = [{
    `(` $labels `)` (`property` $property^)? (`of` `scan` $propertyScan^)? attr-dict `:` qualified(type($result))
  }];
}

/**
* Aggregates: Sum / Min / Max / Avg / StDev / StDevP
*/
// The value-reducing counterparts of db.count. Each consumes the whole dataflow of
// one property column and produces a single-row column holding the reduction.
// Cypher aggregates ignore nulls, so they reduce only the non-null rows.
//
// Unlike count(*), which tallies rows regardless of value and so accepts an ID
// column, these reduce the values themselves, so the input must be a property
// value column. The result value type follows Cypher and is resolved during
// lowering: sum and min/max keep the input's type, avg is a float over numbers, and the
// result is null when no non-null row was seen (except sum, which is 0). Like
// db.count the input and result types are unrelated, so there is no verifier.
//
// One op per reduction, the way db.count is its own op: the op name is the
// aggregate, so no kind attribute is needed and each reads like `db.sum(%x)`. They
// share this base since only the name differs.
//
// `distinct` is Cypher's sum(DISTINCT x) / avg(DISTINCT x): the reduction charges
// each distinct value once, however many rows carry it. As on db.count it stays one
// op, since the reduction is unchanged over a deduplicated column - it lowers to the
// DISTINCT seen-set (nl.distinct / nl.distinct_filter) feeding the ordinary
// nl.aggregate_update:
//
//   %s = db.sum(%x) distinct : (!db.column<none>) -> !db.column<none>
//
// db.min and db.max share the flag but the frontend never sets it: dropping repeated
// values cannot move an extremum, so min(DISTINCT x) generates a plain db.min.
class AggregateOp<string mnemonic> : TuringOp<mnemonic, [Pure, AggregateOpInterface]> {
  let summary = "Reduce a column of property values to a single-row result";

  let arguments = (ins Column:$input, UnitAttr:$distinct);
  let results   = (outs Column:$result);

  // One operand and one result, so functional-type(operands, results) prints the
  // `(!db.column<...>) -> !db.column<...>` form, the same as db.count. The distinct
  // flag prints as its keyword ahead of the colon when set, so a plain reduction
  // keeps the form it had before the flag existed.
  let assemblyFormat = [{
    `(` $input `)` (`distinct` $distinct^)? attr-dict `:` functional-type(operands, results)
  }];
}

def Sum : AggregateOp<"sum">;
def Min : AggregateOp<"min">;
def Max : AggregateOp<"max">;
def Avg : AggregateOp<"avg">;

// The sample and the population standard deviation. Both are floats, and 0 rather than
// null where there are too few values to spread: none, or one for the sample.
def StDev : AggregateOp<"stdev">;
def StDevP : AggregateOp<"stdevp">;

def FilterOp : TuringOp<"filter", [Pure, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet"]>]> {
  let summary = "Keep only the rows where the mask is true, filtering carried columns alongside";

  let description = [{
    Applies a boolean mask to a carry set of columns.

    %a2, %b2 = db.filter(%mask, {%a, %b})
  }];

  let arguments = (ins Column:$mask, Variadic<Column>:$columns_to_filter);

  let results   = (outs Variadic<Column>:$filtered_columns);

  let assemblyFormat = [{
    `(` $mask `,` `{` $columns_to_filter `}` `)` attr-dict `:`
    functional-type(operands, results)
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
* GroupAggregate
*/
def GroupAggregate : TuringOp<"group_aggregate", [Pure, DeclareOpInterfaceMethods<AggregateOpInterface, ["getGroupingKeys"]>, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet"]>]> {
  let summary = "Group rows by a key tuple and reduce each group to one row";
  let description = [{
    The declarative counterpart of a Cypher aggregating RETURN / WITH with
    grouping keys - `MATCH ... RETURN k1, k2, agg(x), ...`. It consumes the whole
    dataflow of its columns and emits one row per distinct grouping-key tuple: the
    key values followed by each aggregate's reduction over the rows of that group.

    The operands are the grouping-key columns first, then the aggregate-input
    columns; `keys` is how many leading operands are keys (so the rest are the
    aggregate inputs, one per aggregate). `aggregates` names the reduction of each
    aggregate input as GroupAggregateKind keywords (count, count_distinct, sum,
    sum_distinct, min, max, avg, avg_distinct), so its length is the number of
    aggregate inputs. The distinct kinds are Cypher's DISTINCT aggregates: each
    reduces a group's distinct non-null values rather than all of its rows, and keeps
    the result type of the kind it mirrors. min/max have no distinct kind, since
    dropping repeated values cannot move an extremum.

      %a     = db.scan_nodes() : !db.column<!storage.node_id>
      %city  = db.get_node_properties(%a, "city")  : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
      %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<i64>>
      %gcity, %total = db.group_aggregate(%city, %score) keys 1 aggregates [sum]
                         : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<i64>>) -> (!db.column<!storage.nullable<!storage.string>>, !db.column<none>)
      db.output(%gcity, %total) : !db.column<!storage.nullable<!storage.string>>, !db.column<none>

    The results are the grouping-key columns (in operand order, same types) then
    one column per aggregate: count and count_distinct are ui64 counts, sum/min/max
    keep the input's value type and avg widens numbers to f64 - all resolved during lowering,
    the distinct kinds exactly as the kinds they mirror,
    so - like db.count - the aggregate result types are unconstrained here. count(*)
    is expressed by a count over a never-null column (node IDs).

    Grouped aggregation is a pipeline breaker: it must see every row before it can
    emit any, so - like db.sort - it lowers to a hoisted accumulator
    (nl.group_aggregate_buffer), an nl.group_aggregate_update in the producing loop
    that folds each chunk into the per-group state, and an nl.group_aggregate source
    iterator after the loop whose nl.for emits the group rows in chunks. Reducing a
    deterministic read of the graph is itself deterministic, so the op is Pure.
  }];

  // The grouping-key columns then the aggregate-input columns. keyCount is how
  // many leading columns are keys; kinds names each aggregate's reduction, one per
  // trailing column. Both counts are unsigned - never negative - so a negative
  // literal fails to parse rather than reaching the verifier.
  let arguments = (ins Variadic<Column>:$columns,
                       UI64Attr:$keyCount,
                       DenseI64ArrayAttr:$kinds);
  let results   = (outs Variadic<Column>:$results);

  // One result per key column (same type) then one per aggregate; functional-type
  // prints the `(operandTypes) -> resultTypes` form. keyCount prints after `keys`
  // and the kinds after `aggregates`, ahead of the colon and elided from attr-dict
  // since the format names them - the same shape as db.sort. The kinds print as
  // their GroupAggregateKind keywords (`[count, sum, max]`, not `[0, 1, 3]`) through
  // the shared custom<GroupAggregateKinds> directive.
  let assemblyFormat = [{
    `(` $columns `)` `keys` $keyCount `aggregates` custom<GroupAggregateKinds>($kinds) attr-dict `:`
    functional-type(operands, results)
  }];

  // keyCount and kinds must partition the columns, at least one of each, the key
  // results must pass their key columns through, and every kind must be a valid
  // GroupAggregateKind.
  let hasVerifier = 1;
}

/**
* Collect
*/
def Collect : TuringOp<"collect", [Pure, DeclareOpInterfaceMethods<AggregateOpInterface, ["getGroupingKeys"]>, DeclareOpInterfaceMethods<ColumnCarrierOpInterface, ["getCarrySet"]>]> {
  let summary = "Group rows by a key tuple and collect one column per group into a list";
  let description = [{
    Cypher `WITH k..., collect(x) AS xs`. Emits one row per distinct grouping-key
    tuple: the key values then a list of the group's non-null collected values.

      %gcity, %names = db.collect(%city, %name) keys 1
                         : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>) -> (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.list<!storage.nullable<!storage.string>>>)

    keyCount is how many leading columns are keys (0 = ungrouped); the collected value
    columns follow, one per list the projection returns, and then one input per aggregate
    `aggregates` names - the reductions taken over the same groups, as db.group_aggregate
    takes them. Results are the keys (unchanged), one list column per value, then one
    column per aggregate. The value count is what is left over: columns minus the keys
    and the aggregate inputs.

      %gcity, %names, %n = db.collect(%city, %name, %score) keys 1 aggregates [count]
                             : (...) -> (...)

      %names, %ages = db.collect(%name, %age) keys 0
                        : (...) -> (!db.column<!storage.list<!storage.nullable<!storage.string>>>, !db.column<!storage.list<!storage.nullable<i64>>>)

    `distinct` is Cypher's collect(DISTINCT x): a value repeated in a group is collected
    once, so the list holds each of the group's distinct values. It names the value
    columns that dedupe by index, since one projection may collect a column both ways.

      %names = db.collect(%name) keys 0 distinct [0] : (!db.column<!storage.nullable<!storage.string>>) -> !db.column<!storage.list<!storage.nullable<!storage.string>>>
  }];

  // keyCount grouping-key columns, one or more collected columns, then one input per
  // aggregate kinds names; keyCount is unsigned (never negative).
  let arguments = (ins Variadic<Column>:$columns,
                       UI64Attr:$keyCount,
                       OptionalAttr<DenseI64ArrayAttr>:$kinds,
                       OptionalAttr<DenseI64ArrayAttr>:$distinctValues);
  let results   = (outs Variadic<Column>:$results);

  let assemblyFormat = [{
    `(` $columns `)` `keys` $keyCount (`aggregates` custom<GroupAggregateKinds>($kinds)^)?
    (`distinct` custom<ColumnIndices>($distinctValues)^)? attr-dict `:` functional-type(operands, results)
  }];

  // At least one value column after the keys; key results pass through; one list result
  // per value column.
  let hasVerifier = 1;
}

/**
* UnwindCollect
*/
def UnwindCollect : TuringOp<"unwind_collect", [Pure]> {
  let summary = "Group rows by a key tuple, collect one column per group, and re-emit one row per collected element";
  let description = [{
    Cypher `WITH k..., collect(x) AS xs UNWIND xs AS y` where xs feeds only the unwind -
    the fused form, so the list never materializes. Emits one row per collected value:
    the key values (repeated) then that value.

      %gcity, %y = db.unwind_collect(%city, %name) keys 1
                     : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>) -> (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.nullable<!storage.string>>)

    Same operands as db.collect (keyCount keys, one value column); results are the keys
    (unchanged) then one scalar value column of the unwound elements.
  }];

  let arguments = (ins Variadic<Column>:$columns, UI64Attr:$keyCount);
  let results   = (outs Variadic<Column>:$results);

  let assemblyFormat = [{
    `(` $columns `)` `keys` $keyCount attr-dict `:` functional-type(operands, results)
  }];

  // keyCount + 1 columns; key results pass through; trailing result is the value column.
  let hasVerifier = 1;
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
