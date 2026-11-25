#ifndef TURINGDB_NL_JOIN_OPS
#define TURINGDB_NL_JOIN_OPS

include "mlir/Interfaces/InferTypeOpInterface.td"
include "mlir/Interfaces/SideEffectInterfaces.td"

include "NLDialect.td"
include "NLTypes.td"

/**
* CrossProduct
*/
def CrossProduct : NLOp<"cross_product", [Pure, InferTypeOpAdaptor, AttrSizedOperandSegments]> {
  let summary = "Create an iterator over every pair of one loop's chunk rows with another's";

  let description = [{
    Given an outer chunk of N rows and an inner chunk of M rows the product has
    N*M rows: pair k is outer row k/M with inner row k%M, so every outer row meets
    every inner row. Each step of an enclosing nl.for fills one chunk of that
    sequence - each outer column block-repeated and each inner column tiled over
    the pairs the step covers:

      %pairs = nl.cross_product {%a} {%b}
                 : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
      nl.for %ra, %rb in %pairs : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
        ...
      }

    The product is the one operator whose result outgrows its inputs, so it is an
    iterator rather than a straight-line op: laying all N*M rows out at once would
    mean a chunk of 64Ki rows crossed with another holding 4.29e9 rows - 64 GiB for
    two id columns. Chunked, a step holds the chunk size whatever the sides measure,
    which is what the v2 CartesianProductProcessor does with its own cursor.

    The two brace groups are the columns each factor contributes, in db.yield
    order; the iterator's chunk types are the outer columns' followed by the
    inner's, each keeping its input's element type - only the row count changes.
    N and M are read from the first column of each group, so each group must be
    non-empty, and every column of a group must be row-aligned with that first one.

    A `limit` on the enclosing nl.for bounds the product the way it bounds a scan:
    the loop stops once the budget is spent, and - since the pairs come out in
    (outer, inner) order - a step lays out only the prefix the following
    nl.limit_update/nl.output can still emit.
  }];

  // The outer and inner column groups to cross. Two operand groups of independent
  // size need AttrSizedOperandSegments to record each group's size.
  let arguments = (ins Variadic<NLChunk>:$outer_columns,
                       Variadic<NLChunk>:$inner_columns);

  // An iterator over the crossed columns, outer group then inner group; inferred
  // from the operands since the broadcast keeps each chunk's element type.
  let results = (outs NLIterator:$result);

  // The two SSA brace groups mirror the two factors. The result type is inferred,
  // so only the operand chunk types are spelled - the same two groups, each in
  // braces, after the colon (as nl.output spells its chunk types). The
  // operandSegmentSizes attribute is populated and elided automatically by the
  // generated parser/printer.
  let assemblyFormat = [{
    `{` $outer_columns `}` `{` $inner_columns `}` attr-dict `:`
    `{` qualified(type($outer_columns)) `}` `{` qualified(type($inner_columns)) `}`
  }];
}

/**
* HashJoinBuffer
*/
// NOT Pure: each nl.hash_join_buffer is a distinct build side, so two must never
// be CSE'd into one, and an unread one must not be DCE'd - resetting its buffers
// and its key index is a side effect (like nl.sort_buffer).
def HashJoinBuffer : NLOp<"hash_join_buffer", []> {
  let summary = "Create (and reset) the build side of one hash join";
  let description = [{
    Sits at the top of the block the join's two loop nests are rooted in - the
    entry block for a top-level join, the enclosing loop body for one nested in a
    factor - so the build side is emptied once per entry into that block, before
    the build loop refills it. Produces the !nl.hash_join_state handle that
    nl.hash_join_collect fills and nl.hash_join_probe reads.

    `build_key` and `probe_key` are the column indices the two sides match on:
    `build_key` indexes the columns nl.hash_join_collect appends, `probe_key` the
    columns nl.hash_join_probe reads. The buffers' column types are not spelled
    here - they are recovered from the collect that feeds the same handle - so
    only the key spec appears. The result type is buildable and omitted.

      %state = nl.hash_join_buffer build_key 1 probe_key 1
  }];

  let arguments = (ins UI64Attr:$buildKey, UI64Attr:$probeKey);
  let results   = (outs NLHashJoinState:$state);

  let assemblyFormat = "`build_key` $buildKey `probe_key` $probeKey attr-dict";
}

/**
* HashJoinCollect
*/
// NOT Pure: it appends the current chunk's rows to the handle's buffers and
// indexes their keys, so it must run exactly where it sits, once per build-loop
// step, and must not be hoisted, CSE'd or eliminated.
def HashJoinCollect : NLOp<"hash_join_collect", []> {
  let summary = "Append this step's chunk of every build column to a hash join";
  let description = [{
    Runs in the build loop body, once per step. Appends the current chunk of each
    build column to the matching buffer of the handle and records where each row's
    key lands, so that by the time the build loop is exhausted the buffers hold
    every build row and the index answers any probe key. The columns are taken
    together so the buffers stay row-aligned. A row whose key can match nothing is
    buffered like any other but left out of the index: a null matches nothing, and
    so does a NaN, or an embedding holding one, which no `=` calls equal.

      nl.hash_join_collect %state, (%b, %name) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>

    The build side's sibling of nl.sort_collect.
  }];

  // The build handle, then the columns to append. The column chunk types are
  // spelled after the colon (as nl.sort_collect spells its own), since the
  // buffers' element types are recovered from them.
  let arguments = (ins NLHashJoinStateHandle:$state, Variadic<NLChunk>:$columns);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:` qualified(type($columns))
  }];
  let hasVerifier = 1;
}

/**
* HashJoinProbe
*/
// Pure: given a filled build side it deterministically produces the same matched
// rows from the same probe chunk, and it writes only its own result chunks.
def HashJoinProbe : NLOp<"hash_join_probe", [Pure]> {
  let summary = "Create an iterator over each probe row paired with the build rows its key matches";
  let description = [{
    Runs in the probe loop body and stands where nl.cross_product stands in a
    nested loop: it is what makes the join's rows. For each row of the probe chunk
    it looks the row's key up in the handle's index and pairs the row with every
    build row that matches - the probe row's columns repeated once per match,
    beside the matched build row's columns read back from the buffers. A probe row
    whose key is null, or whose key matches nothing, pairs with nothing.

      %pairs = nl.hash_join_probe %state, (%arg0, %key)
          : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>)
         -> !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>>
      nl.for %n, %name, %m in %pairs
          : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>> {
        ...
      }

    A key that many build rows carry makes more pairs than either side holds, so -
    like the cross product - this is an iterator rather than a straight-line op:
    pairing a whole probe chunk at once means a side of 6345 rows sharing one key
    lays out 40e6 rows in a single step. Chunked, a step holds the chunk size
    whatever the fan-out measures, so a step may start and end partway through one
    probe row's matches.

    The iterator's chunks are the probe columns followed by the build columns, so
    they line up with the db.hash_join results this lowers from: the left (probed)
    factor's yielded columns followed by the right (built) factor's. The build
    columns are not operands - they live in the handle - so their chunk types
    cannot be inferred and the iterator type is spelled out, the way nl.sort
    spells its own.

    The whole build side is indexed before any probing, so a probe row's matches
    come out together and in build order at any chunk size - where
    nl.cross_product, re-running the inner nest per outer chunk, interleaves a
    multi-chunk inner side. Neither order is one an ORDER BY constrains, so they
    may differ under a LIMIT.

    A `limit` on the enclosing nl.for bounds the pairing the way it bounds a
    product: the loop stops once the budget is spent, and a step lays out only the
    prefix the following nl.limit_update/nl.output can still emit.
  }];

  let arguments = (ins NLHashJoinStateHandle:$state,
                       Variadic<NLChunk>:$columns);
  let results   = (outs NLIterator:$result);

  let assemblyFormat = [{
    $state `,` `(` $columns `)` attr-dict `:`
    `(` qualified(type($columns)) `)` `->` qualified(type($result))
  }];
  let hasVerifier = 1;
}

#endif // TURINGDB_NL_JOIN_OPS
