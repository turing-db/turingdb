#ifndef TURINGDB_ROW_OPS
#define TURINGDB_ROW_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"

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

#endif // TURINGDB_ROW_OPS
