#ifndef TURINGDB_NL_TYPES
#define TURINGDB_NL_TYPES

include "mlir/IR/AttrTypeBase.td"

include "NLDialect.td"
include "StorageTypes.td"

// Base type. All nested-loop dialect types derived from this.
class NLType<string name, string typeMnemonic, list<Trait> traits = []>
    : TypeDef<NL, name, traits> {
  let mnemonic = typeMnemonic;
}

def NLLimitState : NLType<"LimitState", "limit_state"> {
    let summary = "A row-limit counter handle";
    let description = [{
        Stands for one LIMIT's remaining-row budget. Produced by nl.limit, read by
        the nl.for loops it governs and by nl.output, and decremented by
        nl.limit_update. It is a handle, not a chunk: it holds a count, not values.
        Modeled on !nl.property_type, but mutable - the budget changes as rows are
        emitted.
    }];
}

def NLSkipState : NLType<"SkipState", "skip_state"> {
    let summary = "A row-skip counter handle";
    let description = [{
        Stands for one SKIP's remaining rows-to-drop. Produced by nl.skip, read by
        nl.skip_truncate, and decremented by nl.skip_update. The skip sibling of
        !nl.limit_state: a handle, not a chunk - it holds a count, not values, and
        is mutable as rows are dropped. Unlike a limit it drives no loop early-exit
        (every row past the skip must still be produced), so no nl.for names it.
    }];
}

def NLSortState : NLType<"SortState", "sort_state"> {
    let summary = "A sort accumulator handle";
    let description = [{
        Stands for one ORDER BY's row accumulator. Produced by nl.sort_buffer,
        appended to by nl.sort_collect once per producing-loop step, and drained by
        the nl.for over the nl.sort source iterator, which sorts the accumulated
        rows by the keys and yields them chunk by chunk. It is a handle, not a
        chunk: it owns the growing per-column buffers and the sort spec, not a
        single chunk of values. Modeled on !nl.limit_state, but it accumulates
        rows rather than counting them.
    }];
}

def NLDistinctState : NLType<"DistinctState", "distinct_state"> {
    let summary = "A DISTINCT seen-set handle";
    let description = [{
        Stands for one DISTINCT's set of rows already emitted. Produced by
        nl.distinct and read and mutated by nl.distinct_filter, which drops a row
        whose key is already in the set and records a new one. It is a handle, not
        a chunk: it owns the growing seen-set, not a chunk of values. The streaming
        sibling of !nl.sort_state - it filters rows as they arrive rather than
        accumulating and reordering them, so no nl.for drains it.
    }];
}

def NLCountState : NLType<"CountState", "count_state"> {
    let summary = "A row-count accumulator handle";
    let description = [{
        Stands for one COUNT's running tally. Produced by nl.count, incremented by
        nl.count_update once per producing-loop step, and read by nl.count_result to
        emit the single result row once the loop is exhausted. It is a handle, not a
        chunk: it holds a count, not values. Modeled on !nl.limit_state, but it
        counts up as rows are seen rather than down as a budget is spent - so, unlike
        a limit, no nl.for names it (COUNT drives no early-exit; every row must be
        seen before the tally is final).
    }];
}

def NLAggregateState : NLType<"AggregateState", "aggregate_state"> {
    let summary = "A value-aggregate accumulator handle";
    let description = [{
        Stands for one SUM/MIN/MAX/AVG's running accumulator. Produced by
        nl.aggregate, folded into by nl.aggregate_update once per producing-loop
        step, and read by nl.aggregate_result to emit the single result row once the
        loop is exhausted. The value-reducing sibling of !nl.count_state: where a
        count holds only a tally, this holds the reduced value, so it is parameterized
        by the accumulator's element type (the resolved property primitive - f64 for
        an avg, the input type otherwise). Like a count it drives no early-exit; every
        row must be seen before the accumulator is final, so no nl.for names it.
    }];

    // The accumulator's element type, baked during lowering. avg accumulates as
    // f64; sum/min/max accumulate in the input's own type. It is spelled in the
    // assembly of the ops naming the handle, since - unlike the parameterless
    // state handles - there is no single concrete type to build.
    let parameters = (ins "::mlir::Type":$elementType);
    let assemblyFormat = "`<` $elementType `>`";
}

def NLGroupAggregateState : NLType<"GroupAggregateState", "group_aggregate_state"> {
    let summary = "A grouped-aggregate accumulator handle";
    let description = [{
        Stands for one grouped aggregation's per-group state: a hash table from the
        serialized grouping-key tuple to a group index, the distinct key values per
        group, and one accumulator per aggregate. Produced by
        nl.group_aggregate_buffer, folded into by nl.group_aggregate_update once per
        producing-loop step, and drained by the nl.for over the nl.group_aggregate
        source iterator, which emits one row per group. The grouped sibling of
        !nl.sort_state: it accumulates rows keyed by the grouping tuple rather than
        buffering every row for a global reorder, so it owns the group table and the
        accumulators, not a single chunk of values.
    }];
}

def NLCollectState : NLType<"CollectState", "collect_state"> {
    let summary = "A per-group value-list accumulator handle for one collect";
    let description = [{
        Stands for one collect's per-group state: a hash table from the serialized
        grouping-key tuple to a group index, the distinct key values per group, and -
        the accumulator - a flat buffer of collected values plus, per group, the
        positions of its elements in that buffer. Produced by nl.collect_buffer,
        appended to by nl.collect_update once per producing-loop step, and (later)
        drained one row per element (nl.unwind_collect) or one row per group (nl.collect).
        The list-valued sibling of !nl.group_aggregate_state: where a grouped
        aggregate folds each group's rows to one scalar per aggregate, this keeps
        every value, because the drain will re-expand them.
    }];
}

def NLProcedureState : NLType<"ProcedureState", "procedure_state"> {
    let summary = "A live procedure call handle";
    let description = [{
        Stands for one CALL's procedure state: the procedure's own data (what its
        alloc callback produced), the input columns its arguments are bound to and
        the result columns its yielded return values are bound to. Produced by
        nl.procedure, which prepares it once above the loops, then read by the
        nl.for over nl.procedure_init, which drives the procedure until it
        finishes.
        It is a handle, not a chunk: it owns the call, not a chunk of values - the
        sibling of !nl.sort_state, except the state lives inside the procedure
        rather than in the engine.
    }];
}

def CarriesRows : NativeTypeTrait<"CarriesRows">;

def NLChunk : NLType<"Chunk", "chunk", [CarriesRows]> {
    let summary = "A bounded batch of values";
    let description = "One column chunk filled by a single database iterator step";
    let parameters = (ins "::mlir::Type":$elementType);
    let assemblyFormat = "`<` $elementType `>`";
}

def NLIterator : NLType<"Iterator", "iter"> {
    let summary = "A database iterator producing chunks";
    let description = "Each step of the iterator fills one chunk per parameter type";
    let parameters = (ins ArrayRefParameter<"::mlir::Type">:$chunkTypes);
    let assemblyFormat = "`<` $chunkTypes `>`";
}

def NLPropertyType : NLType<"PropertyType", "property_type"> {
    let summary = "A resolved property type handle";
    let description = [{
        Stands for one property of the graph - storage's PropertyTypeID - once
        its name has been resolved. Produced by nl.get_property_type ahead of
        the loops and consumed by the property fetches, so the name is looked up
        once rather than per chunk. It is not a chunk element type: it names a
        property, it does not hold values.
    }];
}

def NLEdgeType : NLType<"EdgeType", "edge_type"> {
    let summary = "A resolved edge type handle";
    let description = [{
        The edge sibling of !nl.property_type. Stands for one relationship type of
        the graph - storage's EdgeTypeID - once its name has been resolved.
        Produced by nl.get_edge_type ahead of the loops and consumed by the by-type
        edge hops, so the name is looked up once rather than per step when a hop is
        nested in a loop. It is not a chunk element type: it names an edge type, it
        does not hold values.
    }];
}

// NLNodeIDChunk is a type *constraint*, not a new type: it restricts an op
// operand to !nl.chunk<!storage.node_id> without defining anything new. Ops use it
// in their (ins ...) clause where NLChunk would accept a chunk of anything.
//
// It is built from two tablegen pieces:
//
// - Type<predicate, summary, cppClassName>: a constraint over types.
//   * The CPred is a C++ expression evaluated against the operand type
//     ($_self) by the generated op verifier: the type must be a ChunkType
//     and its element type must be NodeIDType.
//   * "chunk of node IDs" is the wording used in verifier error messages.
//   * ::mlir::nl::ChunkType makes the generated operand accessor return
//     ChunkType instead of the generic mlir::Type.
//
// - BuildableType<builderCall>: a recipe to construct, via an OpBuilder
//   ($_builder), the single concrete type satisfying the constraint. This is
//   what lets assembly formats omit the operand type entirely: the parser
//   does not read the type from the text, it builds it from this recipe.
def NLNodeIDChunk : Type<
        CPred<"::llvm::isa<::mlir::nl::ChunkType>($_self) && "
              "::llvm::isa<::mlir::storage::NodeIDType>(::llvm::cast<::mlir::nl::ChunkType>($_self).getElementType())">,
        "chunk of node IDs", "::mlir::nl::ChunkType">,
    BuildableType<"::mlir::nl::ChunkType::get($_builder.getContext(), "
                  "::mlir::storage::NodeIDType::get($_builder.getContext()))">;

// The edge-chunk counterpart of NLNodeIDChunk: constrains an operand to
// !nl.chunk<!storage.edge_id> and carries a BuildableType recipe so an assembly
// format can omit the type. See NLNodeIDChunk above for the piece-by-piece
// breakdown of Type<CPred, summary, cppClass> + BuildableType.
def NLEdgeIDChunk : Type<
        CPred<"::llvm::isa<::mlir::nl::ChunkType>($_self) && "
              "::llvm::isa<::mlir::storage::EdgeIDType>(::llvm::cast<::mlir::nl::ChunkType>($_self).getElementType())">,
        "chunk of edge IDs", "::mlir::nl::ChunkType">,
    BuildableType<"::mlir::nl::ChunkType::get($_builder.getContext(), "
                  "::mlir::storage::EdgeIDType::get($_builder.getContext()))">;

// The mask sibling of NLNodeIDChunk: constrains an operand to
// !nl.chunk<!storage.bool> - a row-by-row flag beside another chunk, as nl.merge's
// pending masks are - and carries the recipe that lets a format omit its type.
def NLBoolChunk : Type<
        CPred<"::llvm::isa<::mlir::nl::ChunkType>($_self) && "
              "::llvm::isa<::mlir::storage::BoolType>(::llvm::cast<::mlir::nl::ChunkType>($_self).getElementType())">,
        "chunk of booleans", "::mlir::nl::ChunkType">,
    BuildableType<"::mlir::nl::ChunkType::get($_builder.getContext(), "
                  "::mlir::storage::BoolType::get($_builder.getContext()))">;

def NLLabelSetIDChunk : Type<
        CPred<"::llvm::isa<::mlir::nl::ChunkType>($_self) && "
              "::llvm::isa<::mlir::storage::LabelSetIDType>(::llvm::cast<::mlir::nl::ChunkType>($_self).getElementType())">,
        "chunk of label set IDs", "::mlir::nl::ChunkType">,
    BuildableType<"::mlir::nl::ChunkType::get($_builder.getContext(), "
                  "::mlir::storage::LabelSetIDType::get($_builder.getContext()))">;

def NLEdgeTypeIDChunk : Type<
        CPred<"::llvm::isa<::mlir::nl::ChunkType>($_self) && "
              "::llvm::isa<::mlir::storage::EdgeTypeIDType>(::llvm::cast<::mlir::nl::ChunkType>($_self).getElementType())">,
        "chunk of edge type IDs", "::mlir::nl::ChunkType">,
    BuildableType<"::mlir::nl::ChunkType::get($_builder.getContext(), "
                  "::mlir::storage::EdgeTypeIDType::get($_builder.getContext()))">;

// Constrains an operand to a resolved !nl.property_type handle, the same way
// NLNodeIDChunk constrains a chunk operand. The single concrete type - there is
// only ever !nl.property_type - is what lets the property fetches omit the
// handle's type in their assembly format, building it from this recipe instead.
def NLPropertyTypeHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::PropertyTypeType>($_self)">,
        "property type handle", "::mlir::nl::PropertyTypeType">,
    BuildableType<"::mlir::nl::PropertyTypeType::get($_builder.getContext())">;

// Constrains an operand to a resolved !nl.edge_type handle, the edge sibling of
// NLPropertyTypeHandle. The single concrete type - there is only ever
// !nl.edge_type - lets the by-type edge hops omit the handle's type in their
// assembly format, building it from this recipe instead.
def NLEdgeTypeHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::EdgeTypeType>($_self)">,
        "edge type handle", "::mlir::nl::EdgeTypeType">,
    BuildableType<"::mlir::nl::EdgeTypeType::get($_builder.getContext())">;

// Constrains an operand to an !nl.limit_state handle, the same way
// NLPropertyTypeHandle constrains the property handle. The single concrete type
// - there is only ever !nl.limit_state - is what lets the loops, nl.limit_update
// and nl.output omit the handle's type in their assembly, building it from this
// recipe instead.
def NLLimitStateHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::LimitStateType>($_self)">,
        "limit state handle", "::mlir::nl::LimitStateType">,
    BuildableType<"::mlir::nl::LimitStateType::get($_builder.getContext())">;

// Constrains an operand to an !nl.skip_state handle, the same way
// NLLimitStateHandle constrains the limit handle. The single concrete type
// - there is only ever !nl.skip_state - is what lets nl.skip_update and
// nl.skip_truncate omit the handle's type in their assembly, building it from
// this recipe instead.
def NLSkipStateHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::SkipStateType>($_self)">,
        "skip state handle", "::mlir::nl::SkipStateType">,
    BuildableType<"::mlir::nl::SkipStateType::get($_builder.getContext())">;

// Constrains an operand to an !nl.sort_state handle, the same way
// NLLimitStateHandle constrains the limit handle. The single concrete type -
// there is only ever !nl.sort_state - is what lets nl.sort_collect and nl.sort
// omit the handle's type in their assembly, building it from this recipe instead.
def NLSortStateHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::SortStateType>($_self)">,
        "sort state handle", "::mlir::nl::SortStateType">,
    BuildableType<"::mlir::nl::SortStateType::get($_builder.getContext())">;

// Constrains an operand to an !nl.distinct_state handle, the same way
// NLSortStateHandle constrains the sort handle. The single concrete type - there
// is only ever !nl.distinct_state - is what lets nl.distinct_filter omit the
// handle's type in its assembly, building it from this recipe instead.
def NLDistinctStateHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::DistinctStateType>($_self)">,
        "distinct state handle", "::mlir::nl::DistinctStateType">,
    BuildableType<"::mlir::nl::DistinctStateType::get($_builder.getContext())">;

// Constrains an operand to an !nl.count_state handle, the same way
// NLSortStateHandle constrains the sort handle. The single concrete type - there
// is only ever !nl.count_state - is what lets nl.count_update omit the handle's
// type in its assembly, building it from this recipe instead.
def NLCountStateHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::CountStateType>($_self)">,
        "count state handle", "::mlir::nl::CountStateType">,
    BuildableType<"::mlir::nl::CountStateType::get($_builder.getContext())">;

// Constrains an operand to an !nl.group_aggregate_state handle, the same way
// NLSortStateHandle constrains the sort handle. The single concrete type - there
// is only ever !nl.group_aggregate_state - is what lets nl.group_aggregate_update
// and nl.group_aggregate omit the handle's type in their assembly, building it
// from this recipe instead.
def NLGroupAggregateStateHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::GroupAggregateStateType>($_self)">,
        "group aggregate state handle", "::mlir::nl::GroupAggregateStateType">,
    BuildableType<"::mlir::nl::GroupAggregateStateType::get($_builder.getContext())">;

// Constrains an operand to an !nl.collect_state handle, the same way
// NLGroupAggregateStateHandle constrains the grouped accumulator. The single
// concrete type - there is only ever !nl.collect_state - is what lets
// nl.collect_update omit the handle's type in its assembly, building it from this
// recipe instead.
def NLCollectStateHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::CollectStateType>($_self)">,
        "collect state handle", "::mlir::nl::CollectStateType">,
    BuildableType<"::mlir::nl::CollectStateType::get($_builder.getContext())">;

// Constrains an operand to an !nl.procedure_state handle, the same way
// NLSortStateHandle constrains the sort handle. The single concrete type - there
// is only ever !nl.procedure_state - is what lets nl.procedure_init omit the
// handle's type in its assembly, building it from this recipe instead.
def NLProcedureStateHandle : Type<
        CPred<"::llvm::isa<::mlir::nl::ProcedureStateType>($_self)">,
        "procedure state handle", "::mlir::nl::ProcedureStateType">,
    BuildableType<"::mlir::nl::ProcedureStateType::get($_builder.getContext())">;

#endif // TURINGDB_NL_TYPES
