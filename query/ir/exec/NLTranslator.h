#pragma once

#include <memory>
#include <optional>

#include "mlir/Dialect/Func/IR/FuncOps.h"
#include "llvm/ADT/DenseMap.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/ADT/StringRef.h"

#include "columns/ColumnOperator.h"
#include "ProcedureTypeVector.h"
#include "metadata/PropertyType.h"

#include "NLOps.h"

#include "NLProgram.h"

namespace db {

class LocalMemory;
class GraphView;
class MetadataBuilder;
class NLSystemTranslator;
class Procedure;
class ProcedureContext;

// Translates an MLIR func.func in the nl dialect into an NLProgram
class NLTranslator {
public:
    NLTranslator(NLProgram* program,
                 LocalMemory* memory,
                 const GraphView* view,
                 MetadataBuilder* metadataBuilder = nullptr,
                 const ProcedureContext* procedureContext = nullptr);
    ~NLTranslator();

    void translate(const mlir::func::FuncOp& function);

private:
    // Kind of iterators passed to each for loop
    enum class IteratorKind {
        ScanNodes,
        ScanNodesByLabel,
        ConstScanNodes,
        ScanNodesByPropertyValue,
        ScanEdges,
        ScanEdgesByType,
        ScanEdgesBySourceLabel,
        ScanEdgesByTargetLabel,
        GetOutEdges,
        GetInEdges,
        GetEdges,
        GetOutEdgesByType,
        GetInEdgesByType,
        GetOutEdgesByLabel,
        GetInEdgesByLabel,
        ExplorePaths,
        Sort,
        GroupAggregate,
        UnwindCollect,
        Collect,
        ShortestPath,
        UnwindConst,
        LoadCSV,
        VectorSearch,
        Unwind,
        ProcedureInit,
        OptionalDrain,
        UnionDrain,
        CrossProduct,
        HashJoinProbe,
        EachRow,
    };

    // Settings of the iterators passed to each for loop
    struct IteratorConfig {
        IteratorKind _kind {IteratorKind::ScanNodes};
        mlir::Value _inputNodes;
        llvm::SmallVector<mlir::Value, 4> _carriedColumns;

        // The accumulator a Sort iterator drains; null for the other kinds.
        NLSortState* _sortState {nullptr};

        // The accumulator a GroupAggregate iterator drains; null for the other kinds.
        NLGroupAggregateState* _groupAggregateState {nullptr};

        // The accumulator an UnwindCollect / Collect iterator drains; null otherwise.
        NLCollectState* _collectState {nullptr};

        NLShortestPathState* _shortestPathState {nullptr};

        // The accumulator an OptionalDrain iterator drains; null for the other kinds.
        NLOptionalState* _optionalState {nullptr};

        // The accumulator a UnionDrain iterator drains; null for the other kinds.
        NLUnionState* _unionState {nullptr};

        // The call a ProcedureInit iterator drives; null for the other kinds.
        NLProcedureState* _procedureState {nullptr};

        // The argument chunks a ProcedureInit iterator hands the procedure, in its
        // declaration order; empty for the other kinds (and for a source call).
        llvm::SmallVector<mlir::Value, 4> _procedureInputs;

        // The two sides a CrossProduct iterator crosses, each in db.yield order;
        // empty for the other kinds.
        llvm::SmallVector<mlir::Value, 4> _crossOuterColumns;
        llvm::SmallVector<mlir::Value, 4> _crossInnerColumns;

        // The step's chunks an EachRow iterator walks one row at a time; empty for the
        // other kinds.
        llvm::SmallVector<mlir::Value, 4> _eachRowColumns;

        // The build side a HashJoinProbe iterator matches against and the probe columns it
        // walks, in db.yield order; null and empty for the other kinds.
        mlir::Value _hashJoinState;
        llvm::SmallVector<mlir::Value, 4> _probeColumns;

        // The label names a ScanNodesByLabel, ScanNodesByPropertyValue or by-label edge
        // iterator filters by, or the end labels an ExplorePaths iterator keeps to; empty
        // for the other kinds. These are views into the op's interned StringAttr storage,
        // which the MLIRContext keeps alive for the whole translation; they are resolved to
        // a LabelSet as soon as the loop is translated.
        llvm::SmallVector<llvm::StringRef, 4> _labels;

        // The edge type names a ScanEdgesByType / GetOutEdgesByType / GetInEdgesByType
        // iterator filters by, as a disjunction, or the single type an ExplorePaths
        // iterator restricts every hop to; empty for the other kinds. Like
        // _labels, views into the op's interned StringAttr storage, which the
        // MLIRContext keeps alive for the whole translation; resolved to EdgeTypeIDs
        // when the loop is translated.
        llvm::SmallVector<llvm::StringRef, 4> _edgeTypes;

        // What an ExplorePaths iterator walks: the direction, the hop bounds (an absent
        // maximum is unbounded) and the hop predicate region, null when the op has none.
        // The region is the op's own, which the module keeps alive for the translation.
        PathExplorationDir _direction {PathExplorationDir::FORWARD};
        uint64_t _minHops {0};
        uint64_t _maxHops {0};
        mlir::Region* _hopRegion {nullptr};
        llvm::SmallVector<mlir::Value, 2> _hopImports;

        // The carried column holding each seed's own end, when the exploration is bound,
        // and whether it reports each (seed, end) pair once instead of every path
        std::optional<uint64_t> _endColumn;
        bool _endsOnSeed {false};
        NLNodeSetState* _endNodeSet {nullptr};
        bool _distinctEnds {false};

        // The node IDs a ConstScanNodes iterator emits; empty for the other kinds.
        // A view into the op's DenseI64ArrayAttr storage, which the MLIRContext
        // keeps alive for the whole translation; resolved to owned NodeIDs when the
        // loop is translated.
        llvm::ArrayRef<int64_t> _nodeIDs;

        // The property name and literal a ScanNodesByPropertyValue iterator matches on;
        // empty and null for the other kinds. Like _labels, views into the op's interned
        // attribute storage; resolved against the schema when the loop is translated.
        llvm::StringRef _property;
        mlir::TypedAttr _propertyValue;

        // The literal list an UnwindConst iterator emits; a null (empty) ListView
        // for the other kinds. Materialized by materializeListView into the
        // query-scoped ListBuffer at config setup.
        ListView _list;

        // What a VectorSearch iterator searches for: the index, how many neighbours to
        // report, and the query vector. Empty and zero for the other kinds. Like _labels
        // and _nodeIDs, views into the op's interned attribute storage, which the
        // MLIRContext keeps alive for the whole execution.
        llvm::StringRef _indexName;
        size_t _neighbourCount {0};
        llvm::ArrayRef<float> _queryVector;

        // What a LoadCSV iterator reads: the file, the fields it produces - each a
        // position or a header name - and the two flags the load carries. Empty and false
        // for the other kinds. Views into the op's interned attribute storage too, so the
        // path and the header names outlive the translation.
        llvm::StringRef _csvPath;
        mlir::ArrayAttr _csvFields;
        bool _csvHasHeaders {false};
        bool _csvSkipOnError {false};

        // The column an Unwind iterator expands, one row per element of each of its
        // cells; null for the other kinds.
        mlir::Value _source;
    };

    NLProgram* _program {nullptr};
    LocalMemory* _memory {nullptr};
    const GraphView* _view {nullptr};
    MetadataBuilder* _metadataBuilder {nullptr};

    // The execution context a procedure reads the graph and the request through,
    // borrowed from the caller: it also carries the registry an nl.procedure's name
    // is resolved against. Null when the caller runs without procedures, which only
    // a function containing an nl.procedure notices.
    const ProcedureContext* _procedureContext {nullptr};
    llvm::DenseMap<mlir::Value, Column*> _valueSlots;

    // The clock is read once for the whole program, so a query calling datetime() more
    // than once answers one instant rather than one per call
    std::optional<DateTime> _queryInstant;

    std::unique_ptr<NLSystemTranslator> _systemTranslator;

    llvm::DenseMap<mlir::Value, IteratorConfig> _iteratorConfigs;

    // Set of nl.create_node result SSA values
    llvm::DenseSet<mlir::Value> _pendingNodeValues;
    llvm::DenseSet<mlir::Value> _pendingEdgeValues;

    // nl.limit handle SSA value -> the runtime counter it produces, so the loops,
    // nl.limit_update and nl.output that name the handle find the same counter
    llvm::DenseMap<mlir::Value, NLLimitState*> _limitStates;

    // nl.skip handle SSA value -> the runtime counter it produces, so nl.skip_update
    // and nl.skip_truncate that name the handle find the same counter
    llvm::DenseMap<mlir::Value, NLSkipState*> _skipStates;

    // nl.sort_buffer handle SSA value -> the runtime accumulator it produces, so
    // nl.sort_collect and the nl.for over nl.sort find the same buffers
    llvm::DenseMap<mlir::Value, NLSortState*> _sortStates;

    // nl.hash_join_buffer handle SSA value -> the runtime build side it produces, so
    // nl.hash_join_collect and nl.hash_join_probe find the same buffers and index
    llvm::DenseMap<mlir::Value, NLHashJoinState*> _hashJoinStates;

    // nl.hash_join_buffer handle SSA value -> the chunk types the collect appended, which
    // are what its buffers were allocated as. The probe declares the build columns among
    // its own results, so this is what those are checked against: the buffers are read
    // back through a gather chosen for their element type, and a result declaring another
    // would read one column's rows as another's.
    llvm::DenseMap<mlir::Value, llvm::SmallVector<mlir::Type, 4>> _hashJoinBuildTypes;

    // nl.distinct handle SSA value -> the runtime seen-set it produces, so the
    // nl.distinct_filter that names the handle finds the same set
    llvm::DenseMap<mlir::Value, NLDistinctState*> _distinctStates;

    // nl.count handle SSA value -> the runtime tally it produces, so the
    // nl.count_update and nl.count_result that name the handle find the same counter
    llvm::DenseMap<mlir::Value, NLCountState*> _countStates;

    // nl.aggregate handle SSA value -> the runtime accumulator it produces, so the
    // nl.aggregate_update and nl.aggregate_result that name the handle find the same
    // accumulator
    llvm::DenseMap<mlir::Value, NLAggregateState*> _aggregateStates;

    // nl.group_aggregate_buffer handle SSA value -> the runtime accumulator it
    // produces, so nl.group_aggregate_update and the nl.for over nl.group_aggregate
    // find the same group table and per-group state
    llvm::DenseMap<mlir::Value, NLGroupAggregateState*> _groupAggregateStates;

    // nl.collect_buffer handle SSA value -> the runtime accumulator it produces, so
    // nl.collect_update (and later the drain) that name the handle find the same group
    // table and per-group lists
    llvm::DenseMap<mlir::Value, NLCollectState*> _collectStates;

    llvm::DenseMap<mlir::Value, NLNodeSetState*> _nodeSetStates;
    llvm::DenseMap<mlir::Value, NLShortestPathState*> _shortestPathStates;

    // nl.optional_buffer handle SSA value -> the runtime accumulator it produces, so
    // nl.optional_collect and the nl.for over nl.optional_drain find the same buffers and
    // matched flags
    llvm::DenseMap<mlir::Value, NLOptionalState*> _optionalStates;
    llvm::DenseMap<mlir::Value, NLUnionState*> _unionStates;
    llvm::DenseMap<mlir::Value, NLExistsState*> _existsStates;

    // nl.pattern_comprehension_state handle SSA value -> the accumulator it names, so the
    // collect staging the matches and the build reading them find the one the buffer opened
    llvm::DenseMap<mlir::Value, NLPatternComprehensionState*> _patternComprehensionStates;

    // nl.procedure handle SSA value -> the runtime call it produces, so every op that
    // names the handle - the nl.for over nl.procedure_init - drives the same procedure
    llvm::DenseMap<mlir::Value, NLProcedureState*> _procedureStates;

    void translateBlock(mlir::Block& block, NLStmtContainer* body);
    void translateFor(mlir::nl::For forLoop, NLStmtContainer* body);
    void translateScanLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body);

    // Translate the nl.for over an nl.scan_nodes_by_label iterator: resolve the
    // config's label names against the schema into a LabelSet (marking the scan
    // unmatchable if any name is absent), allocate the node loop variable, and
    // record the label-filtered scan loop statement in body
    void translateScanByLabelLoop(const IteratorConfig& config,
                                  mlir::Block& loopBody,
                                  NLLimitState* limit,
                                  NLStmtContainer* body);

    // Translate the nl.for over an nl.const_scan_nodes iterator: allocate the node
    // loop variable, resolve the config's constant i64 list into the owned NodeIDs
    // the loop emits, and record the const-scan loop statement in body. The fixed
    // sibling of translateScanLoop - a source loop that emits a known set of node
    // IDs rather than walking the graph.
    void translateConstScanLoop(const IteratorConfig& config,
                                mlir::Block& loopBody,
                                NLLimitState* limit,
                                NLStmtContainer* body);

    void translateScanByPropertyValueLoop(const IteratorConfig& config,
                                          mlir::Block& loopBody,
                                          NLLimitState* limit,
                                          NLStmtContainer* body);

    // Resolve label names into the LabelSet a scan filters by. A node matches when its
    // label set is a superset of this one, so the names are ANDed; false when a name is
    // absent from the schema, which leaves the conjunction unsatisfiable.
    bool resolveLabelSet(llvm::ArrayRef<llvm::StringRef> labels, LabelSet& labelset) const;

    // Translate the nl.for over an nl.unwind_const iterator: allocate the single
    // value loop variable (a nullable value column for a homogeneous list, a
    // ColumnVector<ListElementView> for a heterogeneous one) and record the
    // unwind-const loop statement in body. The literal sibling of
    // translateConstScanLoop - a source loop that streams a fixed ListView one chunk
    // at a time rather than a set of node IDs.
    void translateUnwindConstLoop(const IteratorConfig& config,
                                  mlir::Block& loopBody,
                                  NLLimitState* limit,
                                  NLStmtContainer* body);

    // Translate the nl.for over an nl.load_csv iterator: allocate one owning string loop
    // variable per field the load produces, gather them into the row the parser fills,
    // and record the load loop statement in body. The file sibling of
    // translateUnwindConstLoop - a source loop whose rows come from a CSV file rather
    // than from a fixed list.
    void translateLoadCSVLoop(const IteratorConfig& config,
                              mlir::Block& loopBody,
                              NLLimitState* limit,
                              NLStmtContainer* body);

    // Translate the nl.for over an nl.vector_search iterator: allocate the two nullable
    // value loop variables (the neighbour IDs and the distances they scored) and record
    // the vector-search loop statement in body. The neighbour sibling of
    // translateUnwindConstLoop - a source loop whose rows come from a vector index
    // rather than from a fixed list.
    void translateVectorSearchLoop(const IteratorConfig& config,
                                   mlir::Block& loopBody,
                                   NLLimitState* limit,
                                   NLStmtContainer* body);

    // Translate the nl.for over an nl.unwind iterator: allocate the element loop
    // variable and one per carried column, pick the handlers that read the source
    // column's shape, and record the expansion loop statement. The per-row sibling of
    // translateUnwindConstLoop - it reads an input chunk and carries the rows in flight.
    void translateUnwindLoop(const IteratorConfig& config,
                             mlir::Block& loopBody,
                             NLLimitState* limit,
                             NLStmtContainer* body);

    // The drain filling an nl.unwind's element chunk from a list column: the chunk's own
    // element type is what the list resolved to, so it names the column the drain fills
    static NLUnwindElementEmitFunction selectListUnwindEmit(mlir::Type chunkType, bool sourceIsNullable);

    // The handlers a column of @param sourceElement gives its elements up through: how
    // many each of its cells contributes, and how the element chunk of @param
    // elementChunkType is filled from them. A column whose cells are the elements already
    // has no drain of its own, so @param elementEmit comes back null and its element chunk
    // is gathered like any carried column.
    static void selectElementDrain(mlir::Type sourceElement,
                                   mlir::Type elementChunkType,
                                   NLUnwindElementCountFunction& elementCount,
                                   NLUnwindElementEmitFunction& elementEmit);

    // Whether a cell of a list comprehension's source holds no list, which is what makes
    // its row's result the null `[x IN null | x]` reads as
    static NLCellAbsentFunction selectCellAbsent(mlir::Type sourceElement);

    // Materialize a literal element array - an nl.unwind_const's or an nl.const_list's -
    // into a ListView in the query-scoped ListBuffer, which the unwind loop then reads
    // chunk by chunk and a constant list keeps whole. A nested array is materialized
    // first and kept as one element of the list holding it.
    ListView materializeListView(mlir::ArrayAttr elements);

    MapView materializeMapView(mlir::DictionaryAttr entries);

    // The characters of @param text, copied into the query's string buffer. The list and
    // map buffers store a view rather than the characters, and an attribute's characters die
    // with the MLIR context when the query returns - before an embedded client reads the rows
    std::string_view ownedCharacters(llvm::StringRef text);

    // The floats of @param embedding, copied into the query's embedding buffer, for the
    // same reason ownedCharacters copies characters
    types::Embedding::Primitive ownedFloats(llvm::ArrayRef<float> embedding);

    // The bytes the elements' values occupy, which sizes the region materializeListView
    // reserves before writing them
    static size_t listValueBytes(mlir::ArrayAttr elements);

    // The bytes the entries' values occupy, which sizes the region materializeMapView
    // reserves before writing them
    static size_t mapValueBytes(mlir::DictionaryAttr entries);

    // Translate the nl.for over an nl.scan_edges iterator: allocate the four
    // fixed edge loop variables (sources, edge IDs, edge type IDs, targets) and
    // record the edge-scan loop statement in body. The edge sibling of
    // translateScanLoop - a source loop with no input chunk and no carry set.
    void translateScanEdgesLoop(mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body);
    void translateScanEdgesByTypeLoop(const IteratorConfig& config, mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body);
    void translateScanEdgesByLabelLoop(const IteratorConfig& config, mlir::Block& loopBody, NLLimitState* limit, NLStmtContainer* body, NLHandlerFunction executor);

    // Record the iterator config of one of the four by-label edge scans: they differ in
    // which endpoint carries the labels, which the kind names, and each reads its label
    // list the same way.
    template <typename ScanOp>
    void bindScanEdgesByLabel(ScanOp scan, IteratorKind kind);

    // Record the iterator config of one of the two by-label edge hops: they differ in the
    // direction they walk, which the kind names, and each reads its input chunk, carry set
    // and label list the same way.
    template <typename HopOp>
    void bindGetEdgesByLabel(HopOp hop, IteratorKind kind);

    void translateEdgeLoop(const IteratorConfig& config,
                           mlir::Block& loopBody,
                           NLLimitState* limit,
                           NLStmtContainer* body);

    // What this change knows an edge type name by: the graph's schema, plus the names a
    // CREATE earlier in the program introduced, which live in the change's own schema and
    // nowhere else until the commit
    std::optional<EdgeTypeID> findEdgeType(llvm::StringRef name) const;

    // The EdgeTypeIDs a by-type loop filters by. The names are a disjunction, so one
    // absent from the schema drops out rather than failing the query; a set nothing
    // resolves into matches no edge and the loop emits nothing.
    void resolveEdgeTypes(llvm::ArrayRef<llvm::StringRef> names,
                          llvm::SmallVectorImpl<EdgeTypeID>& resolved) const;
    void resolveEdgeTypes(mlir::ArrayAttr names,
                          llvm::SmallVectorImpl<EdgeTypeID>& resolved) const;
    std::optional<LabelID> findLabel(llvm::StringRef name) const;
    std::optional<PropertyType> findPropertyType(llvm::StringRef name) const;

    // Records on @param data the ID of every label set this change knows that a node must
    // carry at least @param constraint to be in
    void collectMatchingLabelSets(const LabelSet& constraint, NLCheckLabelConstraintData* data) const;

    // Translate the nl.for over an nl.explore_paths iterator: allocate the seed, end and
    // path loop variables, resolve the edge type name and the end labels against the
    // schema (marking the exploration unmatchable if one is absent), bind the carry set,
    // translate the hop region - when there is one - into the loop data's hop statements
    // over three loop-owned columns, and record the exploration loop statement in body
    void translateExplorePathsLoop(const IteratorConfig& config,
                                   mlir::Block& loopBody,
                                   NLLimitState* limit,
                                   NLStmtContainer* body);

    // Allocate the filtered output of every carried column of an expansion loop, bound to
    // the loop variables from firstCarriedArgument on, with the gather that fills it
    void bindCarriedColumns(const IteratorConfig& config,
                            mlir::Block& loopBody,
                            size_t firstCarriedArgument,
                            NLExpansionLoopData* loopData);

    // Translate an nl.expand_path: allocate the list column its rows expand into, and an
    // nl.path_length: allocate the count column its rows are read into
    void translateExpandPath(mlir::nl::ExpandPath expand, NLStmtContainer* body);
    void translatePathLength(mlir::nl::PathLength length, NLStmtContainer* body);
    void translateMakePath(mlir::nl::MakePath makePath, NLStmtContainer* body);

    // Translate an nl.limit: allocate its runtime counter, map the handle to it,
    // and record the reset statement (run each time the enclosing block runs)
    void translateLimit(mlir::nl::Limit limit, NLStmtContainer* body);

    // Translate an nl.limit_update: look up the counter the handle names and
    // record the charge against the representative chunk's row count
    void translateLimitUpdate(mlir::nl::LimitUpdate update, NLStmtContainer* body);

    // Translate an nl.limit_truncate: allocate one fresh output column per input,
    // map each result to its output, and record the prefix-copy statement (each
    // column block-repeated with factor 1 up to the counter's emitThisStep)
    void translateLimitTruncate(mlir::nl::LimitTruncate truncate, NLStmtContainer* body);

    // The runtime counter an optional limit handle names: null for a null handle
    // (an unbounded loop or output), the mapped counter otherwise. Throws if the
    // handle was not produced by an nl.limit translated earlier.
    NLLimitState* limitStateFor(mlir::Value handle) const;

    // Translate an nl.skip: allocate its runtime counter, map the handle to it,
    // and record the reset statement (run each time the enclosing block runs)
    void translateSkip(mlir::nl::Skip skip, NLStmtContainer* body);

    // Translate an nl.skip_update: look up the counter the handle names and record
    // the charge against the representative chunk's row count
    void translateSkipUpdate(mlir::nl::SkipUpdate update, NLStmtContainer* body);

    // Translate an nl.skip_truncate: allocate one fresh output column per input,
    // map each result to its output, and record the suffix-copy statement (each
    // column's surviving suffix copied to the front of the output)
    void translateSkipTruncate(mlir::nl::SkipTruncate truncate, NLStmtContainer* body);

    // The runtime counter a skip handle names. The handle is a required operand of
    // its consumers, so this throws if it was not produced by an nl.skip.
    NLSkipState* skipStateFor(mlir::Value handle) const;

    // Translate an nl.sort_buffer: allocate its runtime accumulator, map the
    // handle to it, and record the reset statement (run each time the block runs)
    void translateSortBuffer(mlir::nl::SortBuffer buffer, NLStmtContainer* body);

    // Translate an nl.sort_collect: allocate one growing buffer per collected
    // column (mapped into the accumulator), build the key comparators from the
    // nl.sort_buffer spec, and record the per-step append statement
    void translateSortCollect(mlir::nl::SortCollect collect, NLStmtContainer* body);

    // Translate the nl.for over an nl.sort iterator: allocate one loop variable
    // per buffer, set up the gather that fills it from the buffer in permutation
    // order, and record the emit-loop statement (sort once, then re-chunk). limit is
    // the counter the drain early-exits on, or null for an unbounded drain.
    void translateSortLoop(const IteratorConfig& config,
                           mlir::Block& loopBody,
                           NLLimitState* limit,
                           NLStmtContainer* body);

    // The runtime accumulator a sort handle names. Throws if the handle was not
    // produced by an nl.sort_buffer translated earlier.
    NLSortState* sortStateFor(mlir::Value handle) const;

    // Translate an nl.hash_join_buffer: allocate its runtime build side, map the
    // handle to it, and record the reset statement (run each time the block runs)
    void translateHashJoinBuffer(mlir::nl::HashJoinBuffer buffer, NLStmtContainer* body);

    // Translate an nl.hash_join_collect: allocate one growing buffer per build
    // column (mapped into the build side), bake the key serializer and the match
    // gate from the key column the nl.hash_join_buffer names, and record the
    // per-step append-and-index statement
    void translateHashJoinCollect(mlir::nl::HashJoinCollect collect, NLStmtContainer* body);

    // Translate an nl.hash_join_probe: allocate one fresh output column per probe
    // and per build column, map each result to its output, bake the probe key's
    // serializer and match gate, and record the per-step probe statement
    void translateHashJoinProbeLoop(const IteratorConfig& config,
                                    mlir::Block& loopBody,
                                    NLLimitState* limit,
                                    NLStmtContainer* body);

    // The runtime build side a hash join handle names. The handle is a required
    // operand of nl.hash_join_collect and nl.hash_join_probe, so this throws if it
    // was not produced by an nl.hash_join_buffer.
    NLHashJoinState* hashJoinStateFor(mlir::Value handle) const;

    // The nl.hash_join_buffer that produced a handle, which carries the two key
    // column indices; throws when the handle came from anything else.
    static mlir::nl::HashJoinBuffer hashJoinBufferOf(mlir::Value handle);

    // Translate an nl.distinct: allocate its runtime seen-set, map the handle to
    // it, and record the reset statement (run each time the block runs)
    void translateDistinctState(mlir::nl::Distinct distinct, NLStmtContainer* body);

    // Translate an nl.distinct_filter: look up the seen-set the handle names,
    // allocate one fresh output column per input, map each result to its output,
    // and record the filter statement (serialize each row's key across all
    // columns, keep the not-yet-seen rows, gather them into the outputs)
    void translateDistinctFilter(mlir::nl::DistinctFilter filter, NLStmtContainer* body);

    // Allocate the fresh output column for one filtered column, map the op result
    // to it, and append it (with its key-append serializer and its survivor
    // gather) to data
    void addDistinctColumn(mlir::Value inputValue,
                           mlir::Value resultValue,
                           NLDistinctFilterData* data);

    // The runtime seen-set a distinct handle names. The handle is a required
    // operand of nl.distinct_filter, so this throws if it was not produced by an
    // nl.distinct.
    NLDistinctState* distinctStateFor(mlir::Value handle) const;

    // Translate an nl.count: allocate its runtime tally, map the handle to it, and
    // record the reset statement (run each time the block runs)
    void translateCountState(mlir::nl::Count count, NLStmtContainer* body);

    // Translate an nl.count_update: look up the tally the handle names and record
    // the charge of the chunk's non-null rows against it (all rows for an ID chunk,
    // the present values for a nullable value chunk)
    void translateCountUpdate(mlir::nl::CountUpdate update, NLStmtContainer* body);

    // Translate an nl.count_result: look up the tally the handle names, allocate the
    // unsigned i64 count chunk it produces, map the op result to it, and record the
    // emit statement (materialize the final tally as the chunk's single row)
    void translateCountResult(mlir::nl::CountResult result, NLStmtContainer* body);

    // Translate an nl.count_scan_rows: resolve each listed conjunction to a label set,
    // allocate the unsigned i64 count chunk the op produces, map the op result to it, and
    // record the single statement that reads the graph's node counts into that chunk's
    // one row - the whole of a count that walks nothing
    void translateCountScanRows(mlir::nl::CountScanRows countScanRows, NLStmtContainer* body);

    // The runtime tally a count handle names. The handle is a required operand of
    // nl.count_update and nl.count_result, so this throws if it was not produced by
    // an nl.count.
    NLCountState* countStateFor(mlir::Value handle) const;

    // Translate an nl.aggregate: allocate its runtime accumulator (a single-row
    // nullable value column of the state handle's element type), map the handle to
    // it, and record the reset statement (run each time the block runs)
    void translateAggregateState(mlir::nl::Aggregate aggregate, NLStmtContainer* body);

    // Translate an nl.aggregate_update: look up the accumulator the handle names and
    // record the fold of the chunk's non-null values into it (selected from the
    // reduction and the input value type)
    void translateAggregateUpdate(mlir::nl::AggregateUpdate update, NLStmtContainer* body);

    // Translate an nl.aggregate_result: look up the accumulator the handle names,
    // allocate the single-row nullable value chunk it produces, map the op result to
    // it, and record the emit statement (materialize the reduced value)
    void translateAggregateResult(mlir::nl::AggregateResult result, NLStmtContainer* body);

    // The runtime accumulator an aggregate handle names. The handle is a required
    // operand of nl.aggregate_update and nl.aggregate_result, so this throws if it
    // was not produced by an nl.aggregate.
    NLAggregateState* aggregateStateFor(mlir::Value handle) const;

    // Translate an nl.group_aggregate_buffer: allocate its runtime accumulator, map
    // the handle to it, and record the reset statement (run each time the block
    // runs). The key buffers and per-aggregate state are allocated by the update,
    // which knows their types, the same way nl.sort_collect allocates the sort
    // buffers.
    void translateGroupAggregateBuffer(mlir::nl::GroupAggregateBuffer buffer, NLStmtContainer* body);

    // Translate an nl.group_aggregate_update: look up the accumulator the handle
    // names, split the collected columns into grouping keys and aggregate inputs
    // (by the keyCount / kinds on the producing nl.group_aggregate_buffer), allocate
    // each key buffer and each aggregate's per-group state with its grow/fold/emit
    // handlers, and record the per-step fold statement.
    // The per-group accumulator one aggregate kind needs over a column: the grow, fold
    // and emit handlers baked from the kind and the input's value type. Shared by the
    // grouped aggregation and the collect that reduces beside its list.
    void buildGroupAggregate(mlir::storage::GroupAggregateKind mlirKind,
                             mlir::Value column,
                             NLGroupAggregateState::Aggregate& aggregate);

    void translateGroupAggregateUpdate(mlir::nl::GroupAggregateUpdate update, NLStmtContainer* body);

    // Translate the nl.for over an nl.group_aggregate iterator: allocate one loop
    // variable per output column (a grouping key or an aggregate result), wire it as
    // that column's emit output, and record the emit-loop statement (re-chunk the
    // groups). limit is the counter the drain early-exits on, or null for an
    // unbounded drain.
    void translateGroupAggregateLoop(const IteratorConfig& config,
                                     mlir::Block& loopBody,
                                     NLLimitState* limit,
                                     NLStmtContainer* body);

    // The runtime accumulator a group-aggregate handle names. Throws if the handle
    // was not produced by an nl.group_aggregate_buffer translated earlier.
    NLGroupAggregateState* groupAggregateStateFor(mlir::Value handle) const;

    // Translate an nl.collect_buffer: allocate its runtime accumulator, map the handle
    // to it, and record the reset statement (run each time the block runs). The key
    // buffers and the value buffer are allocated by the update, which knows their
    // types, the same way nl.group_aggregate_update allocates the group state.
    void translateCollectBuffer(mlir::nl::CollectBuffer buffer, NLStmtContainer* body);

    // Translate an nl.collect_update: look up the accumulator the handle names, split
    // the collected columns into grouping keys and the single value column (by the
    // keyCount on the producing nl.collect_buffer), allocate each key buffer and the
    // flat value buffer with its fold handler, and record the per-step append
    // statement.
    void translateCollectUpdate(mlir::nl::CollectUpdate update, NLStmtContainer* body);

    // The runtime accumulator a collect handle names. Throws if the handle was not
    // produced by an nl.collect_buffer translated earlier.
    NLCollectState* collectStateFor(mlir::Value handle) const;

    // Translate an nl.optional_buffer: allocate the runtime accumulator, map the handle to
    // it, record this step's input chunks and the row tag column, and record the reset
    // statement (run each time the block runs). The row buffers are allocated by the
    // collect, which knows their types, as nl.sort_collect allocates a sort's.
    void translateOptionalBuffer(mlir::nl::OptionalBuffer buffer, NLStmtContainer* body);

    // Translate an nl.optional_collect: allocate one growing buffer per column the pattern
    // contributes with the append that grows it, wire the row tag, and record the per-step
    // statement.
    void translateOptionalCollect(mlir::nl::OptionalCollect collect, NLStmtContainer* body);

    // Translate the nl.for over an nl.optional_drain iterator: allocate one loop variable
    // per column, pair it with the buffer the matched rows come from and - for a column
    // the pattern joined onto - the input chunk a missed row is rebuilt from, and record
    // the emit-loop statement.
    void translateOptionalDrainLoop(const IteratorConfig& config,
                                    mlir::Block& loopBody,
                                    NLLimitState* limit,
                                    NLStmtContainer* body);

    // The runtime accumulator an optional handle names. Throws if the handle was not
    // produced by an nl.optional_buffer translated earlier.
    NLOptionalState* optionalStateFor(mlir::Value handle) const;

    // Translate an nl.union_buffer: allocate the runtime accumulator, map the handle to it
    // and record the reset statement. The buffers are allocated by the first collect.
    void translateUnionBuffer(mlir::nl::UnionBuffer buffer, NLStmtContainer* body);

    // Translate the nl.union_collect of one branch: the first allocates the buffers, and
    // each records the per-step statement appending its columns to them
    void translateUnionCollect(mlir::nl::UnionCollect collect, NLStmtContainer* body);

    // Translate the nl.for over an nl.union_drain iterator: one loop variable per buffer,
    // gathered from it chunk by chunk
    void translateUnionLoop(const IteratorConfig& config,
                            mlir::Block& loopBody,
                            NLLimitState* limit,
                            NLStmtContainer* body);

    NLUnionState* unionStateFor(mlir::Value handle) const;

    // Translate an nl.exists_buffer: allocate the runtime accumulator, map the handle to
    // it, record this step's input chunks and the row tag column, and record the reset
    // statement (run each time the block runs).
    void translateExistsBuffer(mlir::nl::ExistsBuffer buffer, NLStmtContainer* body);

    // Translate an nl.exists_mark: wire the row tag, or the columns an untagged
    // accumulator answers from, and record the per-step statement.
    void translateExistsMark(mlir::nl::ExistsMark mark, NLStmtContainer* body);

    // Translate an nl.exists_result: allocate the boolean chunk the flags are laid out
    // into and record the statement that fills it.
    void translateExistsResult(mlir::nl::ExistsResult result, NLStmtContainer* body);

    // The runtime accumulator an exists handle names. Throws if the handle was not
    // produced by an nl.exists_buffer translated earlier.
    NLExistsState* existsStateFor(mlir::Value handle) const;

    // Translate an nl.pattern_comprehension_buffer: allocate the accumulator this step's
    // matches are staged in and the row tag column the pattern carries
    void translatePatternComprehensionBuffer(mlir::nl::PatternComprehensionBuffer buffer,
                                             NLStmtContainer* body);

    // Translate an nl.pattern_comprehension_collect: bind the tag and the value the
    // pattern's matches contribute, under the read the value column's shape takes
    void translatePatternComprehensionCollect(mlir::nl::PatternComprehensionCollect collect,
                                              NLStmtContainer* body);

    // Translate an nl.pattern_comprehension: allocate the list column the step fills once
    // the pattern's nest has been walked
    void translatePatternComprehension(mlir::nl::PatternComprehension comprehension,
                                       NLStmtContainer* body);

    // The runtime accumulator a pattern comprehension handle names. Throws if the handle
    // was not produced by an nl.pattern_comprehension_buffer.
    NLPatternComprehensionState* patternComprehensionStateFor(mlir::Value handle) const;

    // Translate the nl.for over an nl.unwind_collect iterator: allocate one loop variable per
    // grouping key plus the element value, wire the key outputs and value output onto
    // the shared state, and record the per-element emit-loop statement.
    void translateUnwindCollectLoop(const IteratorConfig& config,
                             mlir::Block& loopBody,
                             NLStmtContainer* body);

    // Translate the nl.for over an nl.collect iterator: allocate one loop variable per
    // grouping key plus the list cell, wire the outputs onto the shared state, and
    // record the per-group emit-loop statement.
    void translateCollectLoop(const IteratorConfig& config,
                              mlir::Block& loopBody,
                              NLStmtContainer* body);

    void translateNodeSetBuffer(mlir::nl::NodeSetBuffer buffer, NLStmtContainer* body);

    void translateNodeSetCollect(mlir::nl::NodeSetCollect collect, NLStmtContainer* body);

    NLNodeSetState* nodeSetStateFor(mlir::Value handle) const;

    void translateShortestPathBuffer(mlir::nl::ShortestPathBuffer buffer, NLStmtContainer* body);

    void translateShortestPathUpdate(mlir::nl::ShortestPathUpdate update, NLStmtContainer* body);

    NLShortestPathState* shortestPathStateFor(mlir::Value handle) const;

    PropertyType resolveShortestPathWeight(mlir::Value stateHandle) const;

    NLShortestPathState* allocShortestPathStateFor(const PropertyType& weight);

    void translateShortestPathLoop(const IteratorConfig& config,
                                   mlir::Block& loopBody,
                                   NLStmtContainer* body);

    // Pool-allocate the flat value buffer for a collected value type: a plain
    // ColumnVector<Primitive> (not nullable - collect drops nulls) that grows as
    // present values are appended across steps.
    Column* allocValueColumnForValueType(ValueType valueType);

    void translateProcedure(mlir::nl::Procedure procedureOp, NLStmtContainer* body);

    // Bind the argument chunks of a call as the procedure's input columns, one per
    // declared argument in declaration order. The chunks are loop variables refilled in
    // place, so binding them once holds for every step.
    void bindProcedureInputs(NLProcedureState* state, mlir::ValueRange inputs);

    void addProcedureCarriedColumns(const IteratorConfig& config,
                                    mlir::Block& loopBody,
                                    size_t yieldCount,
                                    NLProcedureLoopData* loopData);

    // Translate the nl.for over an nl.procedure_init iterator: bind one loop variable
    // per yielded return value as the procedure's result columns, and record the
    // drive-loop statement (run the procedure once per step until it finishes).
    void translateProcedureInitLoop(const IteratorConfig& config,
                                    mlir::Block& loopBody,
                                    NLLimitState* limit,
                                    NLStmtContainer* body);

    // The runtime call a procedure handle names. The handle is a required operand of
    // its consumers, so this throws if it was not produced by an nl.procedure.
    NLProcedureState* procedureStateFor(mlir::Value handle) const;

    // Allocate one result column per yielded return value of the call, bind it to
    // that return value's slot in the procedure's data - so the procedure writes
    // where the engine reads - and map the matching chunk value to it. The chunks are
    // an op's results, or a drive loop's variables; either way there is one per
    // yielded name, in yield order.
    void bindProcedureResults(NLProcedureState* state, mlir::ValueRange chunks);

    // Pool-allocate a result column for one of a procedure's declared return types -
    // an ID column, a value column or a list column - reserving a full chunk so
    // execution stays allocation-free. This is what fixes the column type a procedure
    // writes through, so it mirrors the pipeline engine's allocReturnValues exactly.
    Column* allocColumnForProcedureType(const NamedProcedureType& returnValue);

    // Allocate an emit output column for a group-aggregate output chunk type: an ID
    // column for an ID chunk (a grouping key), a nullable value column for a
    // !storage.nullable<...> chunk (a key or a sum/min/max/avg result), or a
    // ColumnVector<uint64_t> for a ui64 chunk (a count result).
    Column* allocColumnForResultChunkType(mlir::Type chunkType);

    // The key gather-append / range emit-copy for a group-aggregate key column of
    // this chunk type - by chunk kind for an ID chunk, by value type for a nullable
    // value chunk. Used to grow the key buffers and to slice them at emit.
    static NLGroupKeyGatherFunction selectGroupKeyGatherForChunkType(mlir::Type chunkType);
    static NLCopyFunction selectCopyForChunkType(mlir::Type chunkType);

    // Pool-allocate a buffer/loop column matching a chunk type - an ID column for
    // an ID chunk, a nullable value column for a !storage.nullable<...> chunk, a
    // ColumnVector<uint64_t> for a count chunk - and the append/gather/compare handler
    // for that element type. The compare selector throws for chunk types that have no
    // order (an embedding key).
    Column* allocColumnForChunkType(mlir::Type chunkType);
    static NLAppendFunction selectAppendForChunkType(mlir::Type chunkType);

    // The append a sort takes a column of lists with, and nothing for a column of anything
    // else: the buffer holds the lists rather than views of the ones a chunk was handed
    static NLListAppendFunction selectOwnedListAppendForChunkType(mlir::Type chunkType);

    static NLGatherFunction selectGatherForChunkType(mlir::Type chunkType);
    static NLFillNullFunction selectFillNullForChunkType(mlir::Type chunkType);

    // The appender that keys one row of a merge's property value column, chosen from the
    // shape the column comes in - a nullable value chunk, a constant, or a plain chunk -
    // and keying in @param keyType, the type the schema holds the property as.
    static NLKeyAppendFunction selectMergeKeyAppend(mlir::Type chunkType,
                                                    const Column* column,
                                                    ValueType keyType);
    static NLCompareFunction selectCompareForChunkType(mlir::Type chunkType);
    static NLKeyAppendFunction selectKeyAppendForChunkType(mlir::Type chunkType);
    static NLJoinKeyFunctions selectJoinKeyFunctionsForChunkType(mlir::Type chunkType);
    static NLKeyIsMatchableFunction selectKeyMatchableForChunkType(mlir::Type chunkType);

    // The non-null row count handler for a chunk type - the all-rows count for an
    // ID chunk, the present-value count for a !storage.nullable<...> chunk. Used by
    // nl.count_update.
    static NLCountFunction selectCountForChunkType(mlir::Type chunkType);

    // The fold handler for a value reduction over a chunk type. Used by
    // nl.aggregate_update.
    static NLAggregateUpdateFunction selectAggregateUpdateForChunkType(AggregateKind kind,
                                                                      mlir::Type chunkType);

    // The reset a selection's result column needs, and the write one of its branches
    // needs. Both turn on what the result chunk holds: an entity column carries its null
    // in the ID, every other column in an optional. Used by nl.case.
    static NLCaseResetFn selectCaseResetForChunkType(mlir::Type chunkType);
    static NLCaseWriteFn selectCaseWriteForChunkType(mlir::Type resultChunkType,
                                                     const Column* value,
                                                     mlir::Type valueChunkType);

    // Translate an nl.get_node_properties / nl.get_edge_properties: resolve the
    // property name (carried by the nl.get_property_type that produced the
    // handle) to a PropertyTypeID and value type, allocate the nullable value
    // column, and record the with-null fetch statement in body
    void translatePropertyFetch(mlir::Value inputValue,
                                mlir::Value propertyTypeValue,
                                mlir::Value pendingValue,
                                bool allPending,
                                mlir::Value resultValue,
                                bool isNode,
                                NLStmtContainer* body);

    // Whether a chunk holds nothing but entities this change wrote and has not committed.
    // The op says so when a query part cut stands between the create and the read; within
    // one part the chunk is the create's own result, which is what the sets hold
    bool isPendingValue(mlir::Value value, bool isNode) const;

    void translateGetNodeLabelSet(mlir::nl::GetNodeLabelSet op, NLStmtContainer* body);
    void translateGetEdgeTypes(mlir::nl::GetEdgeTypes op, NLStmtContainer* body);

    void translateCheckLabelConstraint(mlir::nl::CheckLabelConstraint op, NLStmtContainer* body);
    void translateCheckEdgeTypeConstraint(mlir::nl::CheckEdgeTypeConstraint op, NLStmtContainer* body);

    void translateCreateNode(mlir::nl::CreateNode createNode, NLStmtContainer* body);

    void translateCreateEdge(mlir::nl::CreateEdge createEdge, NLStmtContainer* body);

    void translateMerge(mlir::nl::Merge merge, NLStmtContainer* body);

    // The label set, candidate index and property values of one chain node the merge
    // looks up and writes, rather than one whose column the query already bound
    void translateMergeNodeSpec(mlir::ArrayAttr labels,
                                mlir::ArrayAttr propNames,
                                mlir::OperandRange propValues,
                                NLMergeData::Node& node);

    // Resolves one property constraint of a merge pattern on both sides: the row's
    // asked-for value column with the appender that keys it, and - when the graph's
    // schema has the property - the scratch column a candidate's value is read back
    // into. Clears @param matchable when it does not, since nothing committed can then
    // carry the property.
    void translateMergeProperty(llvm::StringRef propName,
                                mlir::Value propValue,
                                std::vector<NLMergeProperty>& properties,
                                std::vector<NLMergeScanProperty>& scanProperties,
                                bool& matchable);

    // The scratch column a pending entity's value is read back into for each property of
    // a merge pattern, in the type the property is written with
    void collectWrittenMergeProperties(const std::vector<NLMergeProperty>& properties,
                                       NLMergeScanProperties& writtenProperties);

    // The property a set writes to. A write of a null carries no type on its value chunk,
    // so the property's own type is what it stages, and a name no property in the graph
    // carries has nothing to remove - answered by an invalid property rather than by
    // interning the name.
    PropertyType setPropertyType(llvm::StringRef propName,
                                 mlir::Type valueChunkType,
                                 bool writesNull) const;

    void translateSetNodeProperty(mlir::nl::SetNodeProperty setNodeProperty, NLStmtContainer* body);

    void translateSetEdgeProperty(mlir::nl::SetEdgeProperty setEdgeProperty, NLStmtContainer* body);

    void translateDeleteNode(mlir::nl::DeleteNode deleteNode, NLStmtContainer* body);

    void translateDeleteEdge(mlir::nl::DeleteEdge deleteEdge, NLStmtContainer* body);

    // Allocates singleton column for the constant and assigns MLIR value
    void translateConstant(mlir::nl::Constant constant);

    // Reads the clock into a singleton column, which is the whole of what datetime()
    // does: the op leaves no step behind to run
    void translateCurrentDateTime(mlir::nl::CurrentDateTime currentDateTime);

    // Allocates the row-aligned column a constant is laid out into, and binds the
    // fill that writes the driving relation's row count of its value each step
    void translateBroadcastConstant(mlir::nl::BroadcastConstant broadcast, NLStmtContainer* body);
    // The list sibling of translateConstant: materializes the literals into the query's
    // ListBuffer and allocates a singleton column holding a view of them

    template <ColumnOperator Op, typename OpType>
    void translateBinaryOp(OpType op, NLStmtContainer* body);

    // Binds the index kernel an nl.list_index runs, chosen by what its result holds: a
    // value of the type its list names, or the tagged cell a mixed list holds
    void translateListIndex(mlir::nl::ListIndex index, NLStmtContainer* body);

    void translateNot(mlir::nl::Not notOp, NLStmtContainer* body);
    void translateToNullable(mlir::nl::ToNullable toNullable, NLStmtContainer* body);
    void translateToOwnedString(mlir::nl::ToOwnedString toOwnedString, NLStmtContainer* body);

    void translateCase(mlir::nl::Case caseOp, NLStmtContainer* body);

    // Allocates the list column an nl.make_list writes, and binds the read each element
    // column's cells go into the list buffer through
    void translateMakeList(mlir::nl::MakeList makeList, NLStmtContainer* body);
    void translateMakeMap(mlir::nl::MakeMap makeMap, NLStmtContainer* body);

    // Translate an nl.list_comprehension: allocate the element chunk, one chunk per
    // carried column and the list column the step fills, pick the handlers that read the
    // source column's shape, and translate the body the elements run through
    void translateListComprehension(mlir::nl::ListComprehension comprehension, NLStmtContainer* body);

    // Allocates the list column an nl.range writes, and binds the read each bound column
    // is taken through
    void translateRange(mlir::nl::Range range, NLStmtContainer* body);

    // Translate an nl.list_slice: allocate the column of views the step fills, and bind
    // the reads its list and its bounds are taken through
    void translateListSlice(mlir::nl::ListSlice slice, NLStmtContainer* body);

    // The read one element column of an nl.make_list contributes its cell through, chosen
    // by what the chunk holds
    static NLListItemReadFunction selectListItemRead(mlir::Type chunkType);

    static NLMapValueReadFunction selectMapValueRead(mlir::Type chunkType);

    // The read one bound column of an nl.range is taken through, chosen by the integer
    // the chunk holds
    static NLRangeBoundReadFunction selectRangeBoundRead(mlir::Type chunkType);

    void translateUnaryFunction(mlir::Operation* op, NLStmtContainer* body);

    void translateBinaryFunction(mlir::Operation* op, NLStmtContainer* body);

    void translateFilter(mlir::nl::Filter filter, NLStmtContainer* body);

    void translateOutput(mlir::nl::Output output, NLStmtContainer* body);

    // Whether a step emitting from this block keeps a single row: at function scope the
    // one emission the function makes, and in a loop body only the drain of a keyless
    // accumulator, which carries the single group its reset created.
    bool stepKeepsASingleRow(mlir::Block* block) const;

    // One step per row of the config's columns, each a one-row gather out of them
    void translateEachRowLoop(const IteratorConfig& config,
                              mlir::Block& loopBody,
                              NLLimitState* limit,
                              NLStmtContainer* body);

    // Translate the loop over an nl.cross_product: allocate an output column per
    // crossed column, map each to the matching loop variable, and record the loop
    // that walks the pairs a chunk at a time (outer columns block-repeated, inner
    // columns tiled)
    void translateCrossProductLoop(const IteratorConfig& config,
                                   mlir::Block& loopBody,
                                   NLLimitState* limit,
                                   NLStmtContainer* body);

    // Allocate the output column for one crossed column, map the loop variable to
    // it, and append it (with its block-repeat/tile broadcast) to the outer or
    // inner list of data
    void addCrossColumn(mlir::Value inputValue,
                        mlir::Value resultValue,
                        bool isOuter,
                        NLCrossProductLoopData* data);

    // Allocate the fresh output column for one truncated column, map the op
    // result to it, and append it (with its block-repeat prefix-copy) to data
    void addTruncateColumn(mlir::Value inputValue,
                           mlir::Value resultValue,
                           NLLimitTruncateData* data);

    // Allocate the fresh output column for one skipped column, map the op result
    // to it, and append it (with its range suffix-copy) to data
    void addSkipColumn(mlir::Value inputValue,
                       mlir::Value resultValue,
                       NLSkipTruncateData* data);

    Column* allocColumn(mlir::Value chunkValue);
    Column* allocColumnIfUsed(mlir::Value chunkValue);
    Column* allocColumnForKind(NLChunkKind kind);

    // Allocate a nullable value column for the value type's primitive. The
    // per-step variant reserves a full chunk so the loop body stays
    // allocation-free; the single-row variant reserves one element for an
    // accumulator that never grows past its one row.
    Column* allocOptColumnForValueType(ValueType valueType);
    Column* allocSingleRowOptColumnForValueType(ValueType valueType);
    Column* allocOptColumn(ValueType valueType, size_t reserveSize);

    // A count result is a ui64 tally, the pipeline's one non-nullable value chunk, so
    // it is neither an ID chunk nor a !storage.nullable<...> one and takes a plain
    // ColumnVector<uint64_t>.
    // Whether a nullable chunk's value type is one whose rows own their characters, which
    // the value type alone does not say: labels() and type() format their own text
    // where a string property column borrows the graph's
    static bool isOwnedStringElement(mlir::Type elementType);
    static bool isOwnedStringChunk(mlir::Type chunkType);

    // The per-step variant reserves a full chunk; the sized one is what an accumulator
    // holding a single row takes.
    Column* allocOptOwnedStringColumn();
    Column* allocOptOwnedStringColumn(size_t reserveSize);

    static bool isMaskElementType(mlir::Type elementType);

    Column* allocMaskColumn();

    static bool isEntityIDElement(mlir::Type elementType);

    static bool isIDElement(mlir::Type elementType);
    Column* allocOptIDColumn(NLChunkKind kind);

    static bool isPlainValueElementType(mlir::Type elementType);
    Column* allocPlainColumn(ValueType valueType);
    ColumnVector<uint64_t>* allocCountColumn();

    // Allocate a type-erased column of tagged scalars - the shape a heterogeneous
    // unwind emits and a cross product broadcasts - reserving a full chunk.
    // A column of list cells, each a view over the query's list buffer
    Column* allocListColumn();
    Column* allocOptListColumn();
    Column* allocEntityListColumn();

    Column* allocListElementColumn();
    Column* allocOptListElementColumn();

    Column* getColumn(mlir::Value chunkValue) const;

    // The mask an optional boolean chunk operand resolves to, or null for an absent one
    const ColumnMask* getMaskColumn(mlir::Value chunkValue) const;
    static NLChunkKind getChunkKind(mlir::Type chunkType);
    static NLChunkKind chunkKindFromElementType(mlir::Type elementType);
};

}
