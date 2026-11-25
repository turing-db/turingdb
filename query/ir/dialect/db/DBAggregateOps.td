#ifndef TURINGDB_AGGREGATE_OPS
#define TURINGDB_AGGREGATE_OPS

include "mlir/Interfaces/SideEffectInterfaces.td"

include "DBDialect.td"
include "DBInterfaces.td"
include "DBTypes.td"
include "StorageTypes.td"

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

#endif // TURINGDB_AGGREGATE_OPS
