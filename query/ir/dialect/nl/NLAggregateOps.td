#ifndef TURINGDB_NL_AGGREGATE_OPS
#define TURINGDB_NL_AGGREGATE_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"
include "StorageEnums.td"
include "StorageTypes.td"

/**
* Count
*/
// NOT Pure: each nl.count is a distinct counter, so two must never be CSE'd into
// one, and an unread one must not be DCE'd - resetting the tally is a side effect
// (like nl.limit / nl.sort_buffer).
def Count : NLOp<"count", []> {
  let summary = "Create (and reset) a row-count accumulator, initialized to zero";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level or
    mid-query COUNT - where it zeroes the tally once per execution of that block.
    Produces the !nl.count_state handle nl.count_update increments and
    nl.count_result reads. The result type is buildable and omitted.

      %c = nl.count
  }];

  let results = (outs NLCountState:$state);
  let assemblyFormat = "attr-dict";
}

/**
* CountUpdate
*/
// NOT Pure: it increments the handle, so it must run exactly where it sits, once
// per producing-loop step, and must not be hoisted, CSE'd or eliminated. The count
// sibling of nl.limit_update / nl.sort_collect.
def CountUpdate : NLOp<"count_update", []> {
  let summary = "Charge this step's non-null rows against a count accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Adds the number of non-null
    rows of the current chunk to the tally - all rows of an ID chunk (IDs are never
    null), or the present values of a nullable value chunk (Cypher count(x) counts
    the rows in which x is not null). The sole counter mutator.

      nl.count_update %c, %a : !nl.chunk<!storage.node_id>

    `all_rows` charges the chunk's row count whatever it holds - Cypher count(*),
    which counts rows and reads no value, so the chunk it is anchored on may be a
    nullable one whose nulls a value tally would have skipped:

      nl.count_update %c, %age all_rows : !nl.chunk<!storage.nullable<i64>>
  }];

  // The accumulator handle, then the column whose non-null rows are charged. The
  // handle type is buildable and omitted; the column chunk type is spelled. all_rows
  // is a unit attribute: its presence is the flag.
  let arguments = (ins NLCountStateHandle:$state, NLChunk:$rows, UnitAttr:$all_rows);

  let assemblyFormat = "$state `,` $rows (`all_rows` $all_rows^)? attr-dict `:` qualified(type($rows))";
}

/**
* CountResult
*/
// NOT Pure: it reads the handle's mutable tally (set by the nl.count_updates in
// the producing loop), so it must run exactly where it sits - after that loop -
// and must not be hoisted, CSE'd or eliminated. The count sibling of
// nl.limit_truncate, which likewise reads mutable counter state.
def CountResult : NLOp<"count_result", []> {
  let summary = "Materialize the single result row of a count as a chunk";
  let description = [{
    The emit step of a COUNT. It reads a filled nl.count_state and materializes the
    tally directly as a single-row chunk - one unsigned i64. A count is a
    non-negative number that is never null, so the chunk is a plain !nl.chunk<ui64>,
    not a nullable value chunk.

    Unlike nl.sort it is not a source iterator: a COUNT collapses to exactly one
    row, so there is nothing to iterate. It produces the chunk in place, at function
    scope after the producing loop, and nl.output consumes it there - no emit loop:

      %r = nl.count_result(%c) : !nl.chunk<ui64>
      nl.output(%r) : !nl.chunk<ui64>

    The result chunk type is fixed (a ui64) but cannot be inferred from the
    parameterless state handle, so it is spelled after the colon. Placing
    nl.count_result after the producing loop guarantees the tally is final.
  }];

  let arguments = (ins NLCountStateHandle:$state);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

/**
* CountScanRows
*/
def CountScanRows : NLOp<"count_scan_rows", [Pure, InferTypeOpAdaptor]> {
  let summary = "Materialize the node counts of whole scans as their single-row product";
  let description = [{
    Imperative counterpart of db.count_scan_rows. Reads one node count per label
    conjunction listed in `labels` and materializes their product as a single-row
    chunk - one unsigned i64, the chunk nl.count_result emits:

      %n = nl.count_scan_rows([["Reaction"], ["Pathway"]])

    Unlike a count it opens no loop and reads no chunk: the counts come from the
    graph's own per-label node counts, so there is no relation to walk and nothing
    to accumulate. It produces the chunk in place at the top of its dataflow's root: at
    function scope, or in the loop body of a body run once per row.

    `property`, when given, narrows the scan `propertyScan` names to the nodes holding
    that property - the count(a.name) form, answered off the index the graph keeps of
    each property by the label set of its holders. The scan is counted from 0 and
    defaults to the first:

      %m = nl.count_scan_rows([["Person"]]) property "name"
      %p = nl.count_scan_rows([["Reaction"], ["Pathway"]]) property "stId" of scan 1

    `labels` is the list db.count_scan_rows carried; translation resolves each name
    to a storage LabelID, the same way an nl.scan_nodes_by_label name is resolved, and
    the property name to a storage PropertyTypeID. A name absent from the schema leaves
    the conjunction unsatisfiable, so that factor counts 0 and the product is 0. An
    empty conjunction filters nothing and counts every node.

    The chunk is always a !nl.chunk<ui64> - a non-negative tally that is never null -
    so the result type is inferred, not spelled out.
  }];

  let arguments = (ins LabelRefArrayArrayAttr:$labels,
                       OptionalAttr<StrAttr>:$property,
                       OptionalAttr<UI64Attr>:$propertyScan);
  let results   = (outs NLChunk:$result);
  let hasVerifier = 1;

  let assemblyFormat = "`(` $labels `)` (`property` $property^)? (`of` `scan` $propertyScan^)? attr-dict";
}

/**
* Aggregate
*/
// NOT Pure: the value-reducing sibling of nl.count. Each nl.aggregate is a
// distinct accumulator, so two must never be CSE'd into one, and an unread one
// must not be DCE'd - resetting the accumulator is a side effect.
def Aggregate : NLOp<"aggregate", []> {
  let summary = "Create (and reset) a value-aggregate accumulator";
  let description = [{
    The value-reducing counterpart of nl.count. Sits at the top of the scope it
    governs, where it resets the accumulator once per execution of that block, and
    produces the !nl.aggregate_state handle nl.aggregate_update folds into and
    nl.aggregate_result reads. `$kind` selects the reduction (sum/min/max/avg); the
    state type carries the accumulator element type, so the result type is spelled.

      %s = nl.aggregate sum : !nl.aggregate_state<i64>
  }];

  let arguments = (ins AggregateKind:$kind);
  let results   = (outs NLAggregateState:$state);
  let assemblyFormat = "$kind attr-dict `:` qualified(type($state))";

  // avg accumulates a running sum as f64, so its accumulator state must be f64;
  // the other kinds accumulate in the value's own type.
  let hasVerifier = 1;
}

/**
* AggregateUpdate
*/
// NOT Pure: it folds the chunk into the handle, so it must run exactly where it
// sits, once per producing-loop step. The value sibling of nl.count_update.
def AggregateUpdate : NLOp<"aggregate_update", []> {
  let summary = "Fold this step's non-null rows into a value-aggregate accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Reduces the present (non-null)
    values of the current nullable value chunk into the accumulator - adding them
    (sum/avg) or keeping the extreme (min/max) - matching Cypher, where these
    aggregates ignore null rows. The sole accumulator mutator.

      nl.aggregate_update sum %s, %v : !nl.aggregate_state<i64>, !nl.chunk<!storage.nullable<i64>>

    The input is always a nullable value chunk (a property fetch); its value type
    need not match the accumulator's - an avg of an i64 column folds into an f64
    accumulator, so both types are spelled after the colon.
  }];

  let arguments = (ins NLAggregateState:$state, NLChunk:$rows, AggregateKind:$kind);

  let assemblyFormat = [{
    $kind $state `,` $rows attr-dict `:` qualified(type($state)) `,` qualified(type($rows))
  }];

  // The fold handler is picked from this op's kind but the accumulator was reset by
  // the producing nl.aggregate's kind, and the handler reads the accumulator as its
  // own value type - so a kind that disagrees with the producer, an input that is
  // not a nullable value chunk, or a state element type that does not match the
  // reduction would fold into a wrongly-typed or wrongly-initialized accumulator.
  // The verifier reconciles all three so such IR is rejected rather than crashing.
  let hasVerifier = 1;
}

/**
* AggregateResult
*/
// NOT Pure: it reads the handle's mutable accumulator (set by the
// nl.aggregate_updates in the producing loop), so it must run exactly where it
// sits - after that loop. The value sibling of nl.count_result.
def AggregateResult : NLOp<"aggregate_result", []> {
  let summary = "Materialize the single result row of a value-aggregate as a chunk";
  let description = [{
    The emit step of a SUM/MIN/MAX/AVG. It reads a filled nl.aggregate_state and
    materializes the reduced value directly as a single-row nullable value chunk.
    Unlike count, an aggregate can be null (min/max/avg of no non-null row), so the
    result is a nullable value chunk, not a plain chunk; sum is never null (0 for an
    empty input) but rides the same nullable representation for uniformity.

      %r = nl.aggregate_result sum (%s) : !nl.aggregate_state<i64> -> !nl.chunk<!storage.nullable<i64>>
      nl.output(%r) : !nl.chunk<!storage.nullable<i64>>

    Like nl.count_result it is not a source iterator: an aggregate collapses to one
    row, so it produces the chunk in place at function scope after the producing
    loop, and nl.output consumes it there - no emit loop.
  }];

  let arguments = (ins NLAggregateState:$state, AggregateKind:$kind);
  let results   = (outs NLChunk:$result);

  let assemblyFormat = [{
    $kind `(` $state `)` attr-dict `:` qualified(type($state)) `->` qualified(type($result))
  }];

  // The emit handler copies the accumulator into the result (or divides for avg),
  // both read as the result's value type, so the result must be a nullable value
  // chunk whose value type matches the reduction, and the kind must match the
  // producing nl.aggregate - the verifier enforces both. The result sibling of
  // AggregateUpdate's verifier.
  let hasVerifier = 1;
}

/**
* GroupAggregateBuffer
*/
// NOT Pure: each nl.group_aggregate_buffer is a distinct accumulator, so two must
// never be CSE'd into one, and an unread one must not be DCE'd - resetting its
// group table is a side effect (like nl.sort_buffer).
def GroupAggregateBuffer : NLOp<"group_aggregate_buffer", []> {
  let summary = "Create (and reset) a per-group accumulator for one grouped aggregation";
  let description = [{
    Sits at the top of the scope it governs - function scope for a top-level
    grouped RETURN - where it re-initializes the group table once per enclosing
    step. Produces the !nl.group_aggregate_state handle that nl.group_aggregate_update
    folds into and the nl.for over nl.group_aggregate drains.

    `keys` is how many of the columns nl.group_aggregate_update collects are the
    grouping keys (the rest are the aggregate inputs, one per aggregate), and
    `aggregates` names each aggregate's reduction as GroupAggregateKind keywords
    (count, sum, min, max, avg). The column types are not spelled here - they are
    recovered from the nl.group_aggregate_update that feeds the same handle - so only
    the shape appears. The result type is buildable and omitted.

      %state = nl.group_aggregate_buffer keys 1 aggregates [sum]
  }];

  // keyCount is how many collected columns are grouping keys; kinds names each
  // aggregate's reduction, one per aggregate input. The two together must
  // partition the collected columns (checked during translation, where the column
  // count is known). kinds prints as its GroupAggregateKind keywords through the
  // shared custom<GroupAggregateKinds> directive (`[sum]`, not `[1]`).
  let arguments = (ins UI64Attr:$keyCount, DenseI64ArrayAttr:$kinds);
  let results   = (outs NLGroupAggregateState:$state);

  let assemblyFormat = "`keys` $keyCount `aggregates` custom<GroupAggregateKinds>($kinds) attr-dict";
  let hasVerifier = 1;
}

/**
* GroupAggregateUpdate
*/
// NOT Pure: it folds the current chunk into the handle's group table, so it must
// run exactly where it sits, once per producing-loop step, and must not be
// hoisted, CSE'd or eliminated. The grouped sibling of nl.sort_collect.
def GroupAggregateUpdate : NLOp<"group_aggregate_update", []> {
  let summary = "Fold this step's chunk of every column into a grouped accumulator";
  let description = [{
    Runs in the producing loop body, once per step. Assigns each row to its group
    (by the serialized tuple of the grouping-key columns, creating a new group the
    first time a tuple is seen), then folds each aggregate-input column's row into
    that group's accumulator. The columns are taken together so the group
    assignment and the per-group folds stay row-aligned.

    The columns are the grouping keys first, then the aggregate inputs, in the
    order nl.group_aggregate_buffer's `keys` / `aggregates` describe; the split and
    the reductions are recovered from that buffer op (which produced the handle),
    so only the column chunk types are spelled here, after the colon.

      nl.group_aggregate_update %state, (%city, %score) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>
  }];

  let arguments = (ins NLGroupAggregateStateHandle:$state, Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
  let hasVerifier = 1;
}

/**
* GroupAggregate
*/
// Pure: given a filled accumulator it deterministically yields the same group
// rows, and it produces no side effect of its own - the emission happens in the
// driving nl.for. Mirrors nl.sort: a source op whose nl.for does the work.
def GroupAggregate : NLOp<"group_aggregate", [Pure]> {
  let summary = "Create a database iterator over the rows of a grouped accumulator";
  let description = [{
    The source op for the emit phase of a grouped aggregation, the grouped
    counterpart of nl.sort. It consumes a filled nl.group_aggregate_state and
    produces an iterator whose steps yield one row per group - the grouping-key
    values followed by each aggregate's reduction - one chunk per output column per
    step, so an enclosing nl.for binds one loop variable per column:

      %groups = nl.group_aggregate(%state) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>>
      nl.for %city, %total in %groups : !nl.iter<...> {
        nl.output(%city, %total) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>
      }

    The iterator chunk types are the grouping-key column types followed by the
    aggregate result types (a ui64 for count, the input's value type for
    sum/min/max, f64 for avg), but cannot be inferred from the parameterless state
    handle, so they are spelled after the colon (as the nl.for iterator type is).
    Placing nl.group_aggregate after the producing loop guarantees every row has
    been folded before the first group is emitted.
  }];

  let arguments = (ins NLGroupAggregateStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

/**
* CollectBuffer
*/
// NOT Pure: a distinct accumulator (no CSE); resetting its group table is a side effect.
def CollectBuffer : NLOp<"collect_buffer", []> {
  let summary = "Create (and reset) a per-group value-list accumulator for one collect";
  let description = [{
    Hoisted above the loops; produces the !nl.collect_state handle nl.collect_update
    appends to and the drain reads. `keys` is how many collected columns are grouping
    keys (0 = one global list); the column types, and how many value columns follow the
    keys, are recovered from the update.

      %buf = nl.collect_buffer keys 1

    `distinct` makes the accumulator charge each of a group's values once - Cypher's
    collect(DISTINCT x) - naming the value columns that dedupe by index. It sits on the
    buffer rather than the update because it is part of what the accumulator is, as its
    key count is: the update carries no spec of its own.

      %buf = nl.collect_buffer keys 0 distinct [1]
  }];

  // keyCount is unsigned; 0 is a valid ungrouped collect, so nothing to verify here
  // (the value-column split is checked during translation). kinds names the reductions
  // taken over the same groups as the list, one per aggregate input the update carries
  // after the value columns, and is absent when the collect reduces nothing beside it.
  let arguments = (ins UI64Attr:$keyCount,
                       OptionalAttr<DenseI64ArrayAttr>:$kinds,
                       OptionalAttr<DenseI64ArrayAttr>:$distinctValues);
  let results   = (outs NLCollectState:$state);

  let assemblyFormat = [{
    `keys` $keyCount (`aggregates` custom<GroupAggregateKinds>($kinds)^)?
    (`distinct` custom<ColumnIndices>($distinctValues)^)? attr-dict
  }];
}

/**
* CollectUpdate
*/
// NOT Pure: appends to the per-group lists; must run where it sits, once per step.
def CollectUpdate : NLOp<"collect_update", []> {
  let summary = "Append this step's rows to the per-group value lists";
  let description = [{
    Assigns each row to its group by the grouping-key tuple and appends its values to
    that group's lists. Columns are the grouping keys then the value columns, per the
    buffer's `keys`; nulls are dropped (Cypher collect ignores them).

      nl.collect_update %buf, (%city, %name) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>
  }];

  let arguments = (ins NLCollectStateHandle:$state, Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];

  let hasVerifier = 1;
}

/**
* UnwindCollect
*/
// Pure: deterministically drains a filled accumulator; the fan-out is in the nl.for.
def UnwindCollect : NLOp<"unwind_collect", [Pure]> {
  let summary = "Create a database iterator that drains a collect accumulator, one row per collected element";
  let description = [{
    Per-element drain of an nl.collect_state: one row per element - the group's key
    values (repeated) then one element. An empty group yields no row (UNWIND of an
    empty list). The iterator chunk types (keys then the value chunk) are spelled after
    the colon, and it is placed after the producing loop.

      %rows = nl.unwind_collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>>
  }];

  let arguments = (ins NLCollectStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

/**
* Collect
*/
// Pure: the per-group sibling of nl.unwind_collect - one row per group, not per element.
def Collect : NLOp<"collect", [Pure]> {
  let summary = "Create a database iterator that drains a collect accumulator, one row per group carrying the whole list";
  let description = [{
    Per-group drain of an nl.collect_state: one row per group - the key values then one
    list cell per value column (a ListView over the group's elements). An empty group
    yields an empty list (unlike nl.unwind_collect). The iterator chunk types (the keys
    then the list cells) are spelled after the colon, and it is placed after the
    producing loop.

      %rows = nl.collect(%buf) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<!storage.string>>>
  }];

  let arguments = (ins NLCollectStateHandle:$state);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = "`(` $state `)` attr-dict `:` qualified(type($result))";
}

#endif // TURINGDB_NL_AGGREGATE_OPS
