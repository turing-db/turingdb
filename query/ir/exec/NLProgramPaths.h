#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <optional>
#include <queue>
#include <span>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "GraphPath.h"
#include "PathView.h"
#include "ID.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorationDir.h"
#include "iterators/PathTargetIndex.h"
#include "list/ListBuffer.h"
#include "list/ListView.h"
#include "list/PathTrie.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"

#include "NLFunctionData.h"
#include "NLColumnFunctions.h"
#include "NLProgramHops.h"

namespace db {

class NLShortestPathLoopData;

// One column a hop predicate reads from outside the hop: the loop's own column, and the
// chunk the region's argument reads, gathered at each candidate's seed row before each run
struct NLHopImport {
    const Column* _source {nullptr};
    Column* _chunk {nullptr};
    NLGatherFunction _gather {nullptr};
};

// nl.explore_paths loop data: the variable-length sibling of NLEdgeLoopData. On top of the
// seed and end chunks it fills the path handle chunk out of the query's trie, and carries
// the exploration's direction, hop bounds and resolved edge type. _matchable is false when
// the type name was absent from the schema, so no hop can match: only the zero-length rows
// of a min of zero are emitted. The hop predicate, when there is one, is the translated
// body of the op's hop region over three loop-owned columns, ending in the mask chunk, or
// the op's hop labels, resolved like the end labels.
// The end labels, when the op carries them, are resolved here too; _endMatchable is false
// when one was absent from the schema, so no path can end and nothing is emitted. The
// distance index that prunes the walk is owned here so every chunk of the loop shares it;
// the target index of a bound end is owned here too, rebuilt for each chunk's targets.
// The nodes one column holds, gathered chunk by chunk while the loop that binds it runs. The
// walk that ends on the set reads it sorted and without duplicates, which is the order its
// membership test and its target index both want.
class NLNodeSetState {
public:
    NLNodeSetState();
    ~NLNodeSetState();

    void reset();
    void add(const std::vector<NodeID>& nodes);

    std::span<const NodeID> getNodes();

private:
    std::vector<NodeID> _nodes;
    bool _ordered {false};
};

// One step of the body an nl.explore_paths walk repeats: the direction and edge types its
// hops follow, and the labels and the predicate they must pass. The predicate reads the
// repetition up to the hop through the node and edge columns of its region, interleaved as
// the region's arguments are; a null column is an argument the region does not read.
class NLExploreStep {
public:
    NLExploreStep(PathExplorationDir direction,
                  bool filtersByType,
                  std::span<const EdgeTypeID> edgeTypes,
                  bool matchable);
    ~NLExploreStep();

    PathExplorationDir getDirection() const { return _direction; }
    bool filtersByType() const { return _filtersByType; }
    std::span<const EdgeTypeID> getEdgeTypes() const { return _edgeTypes; }
    bool isMatchable() const { return _matchable; }

    void setHopLabels(const LabelSet& hopLabels, bool matchable) {
        _hopLabels = hopLabels;
        _filtersByHopLabels = true;
        _hopLabelsMatchable = matchable;
    }

    bool filtersByHopLabels() const { return _filtersByHopLabels; }
    const LabelSet& getHopLabels() const { return _hopLabels; }
    bool areHopLabelsMatchable() const { return _hopLabelsMatchable; }

    bool hasHopFilter() const { return _hopMask != nullptr; }

    void setHopFilter(const Column* hopMask, NLMaskSurvivorFunction hopSurvivors) {
        _hopMask = hopMask;
        _hopSurvivors = hopSurvivors;
    }

    void addHopNode(ColumnNodeIDs* column) { _hopNodes.push_back(column); }
    void addHopEdge(ColumnEdgeIDs* column) { _hopEdges.push_back(column); }
    void addHopImport(const NLHopImport& import) { _hopImports.push_back(import); }

    std::span<ColumnNodeIDs* const> getHopNodes() const { return _hopNodes; }
    std::span<ColumnEdgeIDs* const> getHopEdges() const { return _hopEdges; }
    size_t getHopPosition() const { return _hopEdges.size() - 1; }
    std::span<const NLHopImport> getHopImports() const { return _hopImports; }
    const Column* getHopMask() const { return _hopMask; }
    NLMaskSurvivorFunction getHopSurvivors() const { return _hopSurvivors; }

    NLStmtContainer* getHopStmts() { return &_hopStmts; }
    const NLStmtContainer* getHopStmts() const { return &_hopStmts; }

private:
    PathExplorationDir _direction {PathExplorationDir::FORWARD};
    bool _filtersByType {false};
    std::vector<EdgeTypeID> _edgeTypes;
    bool _matchable {true};

    LabelSet _hopLabels;
    bool _filtersByHopLabels {false};
    bool _hopLabelsMatchable {true};

    std::vector<ColumnNodeIDs*> _hopNodes;
    std::vector<ColumnEdgeIDs*> _hopEdges;
    const Column* _hopMask {nullptr};
    NLMaskSurvivorFunction _hopSurvivors {nullptr};
    std::vector<NLHopImport> _hopImports;
    NLStmtContainer _hopStmts;
};

// The hop bounds count repetitions of the body of steps
class NLExplorePathsLoopData : public NLExpansionLoopData {
public:
    NLExplorePathsLoopData(const ColumnNodeIDs* input,
                           ColumnNodeIDs* sources,
                           ColumnNodeIDs* targets,
                           ColumnVector<PathRef>* paths,
                           PathTrie* trie,
                           uint64_t minHops,
                           uint64_t maxHops)
        : NLExpansionLoopData(input, sources, targets),
        _paths(paths),
        _trie(trie),
        _minHops(minHops),
        _maxHops(maxHops)
    {
    }

    ColumnVector<PathRef>* getPaths() const { return _paths; }
    PathTrie* getTrie() const { return _trie; }
    uint64_t getMinHops() const { return _minHops; }
    uint64_t getMaxHops() const { return _maxHops; }

    NLExploreStep* addStep(PathExplorationDir direction,
                           bool filtersByType,
                           std::span<const EdgeTypeID> edgeTypes,
                           bool matchable);

    size_t getStepCount() const { return _steps.size(); }
    NLExploreStep* getStep(size_t step) const { return _steps[step].get(); }

    bool isMatchable() const;

    void setEndLabels(const LabelSet& endLabels, bool matchable) {
        _endLabels = endLabels;
        _filtersByEndLabels = true;
        _endMatchable = matchable;
    }

    bool filtersByEndLabels() const { return _filtersByEndLabels; }
    const LabelSet& getEndLabels() const { return _endLabels; }
    bool isEndMatchable() const { return _endMatchable; }

    PathDistanceIndex* getDistanceIndex() { return &_distanceIndex; }

    void addSeedsSeen(size_t count) { _seedsSeen += count; }
    size_t getSeedsSeen() const { return _seedsSeen; }

    void setEndNodes(const ColumnNodeIDs* endNodes) { _endNodes = endNodes; }
    const ColumnNodeIDs* getEndNodes() const { return _endNodes; }

    void setEndNodeSet(NLNodeSetState* endNodeSet) { _endNodeSet = endNodeSet; }
    NLNodeSetState* getEndNodeSet() const { return _endNodeSet; }

    PathTargetIndex* getTargetIndex() { return &_targetIndex; }

    void setDistinctEnds() { _distinctEnds = true; }
    bool isDistinctEnds() const { return _distinctEnds; }

private:
    ColumnVector<PathRef>* _paths {nullptr};
    PathTrie* _trie {nullptr};
    uint64_t _minHops {0};
    uint64_t _maxHops {0};
    std::vector<std::unique_ptr<NLExploreStep>> _steps;

    LabelSet _endLabels;
    bool _filtersByEndLabels {false};
    bool _endMatchable {true};
    PathDistanceIndex _distanceIndex;
    size_t _seedsSeen {0};

    const ColumnNodeIDs* _endNodes {nullptr};
    NLNodeSetState* _endNodeSet {nullptr};
    PathTargetIndex _targetIndex;
    bool _distinctEnds {false};
};

// The list a path handle expands to; the interpreter-side counterpart of the MLIR
// storage::PathExpansionKind, which the translator maps onto this
enum class PathExpansionKind {
    Edges,
    Sources,
    Ends,
    Nodes,
};

// nl.expand_path data: one list per row, written into the query's list buffer from the
// trie the handles index. The seeds are read for the sources kind alone.
class NLExpandPathData : public NLFunctionData {
public:
    NLExpandPathData(const ColumnVector<PathRef>* paths,
                     const ColumnNodeIDs* seeds,
                     ColumnVector<ListView>* output,
                     PathExpansionKind kind,
                     bool reversed,
                     PathHopStride hops,
                     const PathTrie* trie,
                     QueryListBuffer* listBuffer)
        : _paths(paths),
        _seeds(seeds),
        _output(output),
        _kind(kind),
        _reversed(reversed),
        _hops(hops),
        _trie(trie),
        _listBuffer(listBuffer)
    {
    }

    const ColumnVector<PathRef>* getPaths() const { return _paths; }
    const ColumnNodeIDs* getSeeds() const { return _seeds; }
    ColumnVector<ListView>* getOutput() const { return _output; }
    PathExpansionKind getKind() const { return _kind; }
    bool isReversed() const { return _reversed; }
    PathHopStride getHops() const { return _hops; }
    const PathTrie* getTrie() const { return _trie; }
    QueryListBuffer* getListBuffer() const { return _listBuffer; }

private:
    const ColumnVector<PathRef>* _paths {nullptr};
    const ColumnNodeIDs* _seeds {nullptr};
    ColumnVector<ListView>* _output {nullptr};
    PathExpansionKind _kind {PathExpansionKind::Edges};
    bool _reversed {false};
    PathHopStride _hops;
    const PathTrie* _trie {nullptr};
    QueryListBuffer* _listBuffer {nullptr};
};

// nl.path_length data: the depth of each handle's entry divided by the steps of the walk's body,
// plus the seed when it counts the nodes, read without expanding the path
class NLPathLengthData : public NLFunctionData {
public:
    NLPathLengthData(const ColumnVector<PathRef>* paths,
                     ColumnVector<uint64_t>* output,
                     size_t steps,
                     bool countsNodes,
                     const PathTrie* trie)
        : _paths(paths),
        _output(output),
        _steps(steps),
        _countsNodes(countsNodes),
        _trie(trie)
    {
    }

    const ColumnVector<PathRef>* getPaths() const { return _paths; }
    ColumnVector<uint64_t>* getOutput() const { return _output; }
    size_t getSteps() const { return _steps; }
    bool countsNodes() const { return _countsNodes; }
    const PathTrie* getTrie() const { return _trie; }

private:
    const ColumnVector<PathRef>* _paths {nullptr};
    ColumnVector<uint64_t>* _output {nullptr};
    size_t _steps {1};
    bool _countsNodes {false};
    const PathTrie* _trie {nullptr};
};

// What a named path is read for; the interpreter-side counterpart of the MLIR
// storage::PathElementsKind, which the translator maps onto this
enum class PathElementsKind {
    Nodes,
    Relationships,
    Length,
};

// The path one row of a column holds - a path column, or a column of tagged cells - and
// nothing where that row holds a null
using NLPathReadFunction = std::optional<PathView> (*)(const Column* paths, size_t row);

// nl.path_elements data: one list of node IDs, one list of edge IDs or one hop count per
// row, read off each path and null where the path is null. The lists are written into the
// query's list buffer.
class NLPathElementsData : public NLFunctionData {
public:
    NLPathElementsData(const Column* paths,
                       NLPathReadFunction read,
                       Column* output,
                       PathElementsKind kind,
                       QueryListBuffer* listBuffer)
        : _paths(paths),
        _read(read),
        _output(output),
        _kind(kind),
        _listBuffer(listBuffer)
    {
    }

    const Column* getPaths() const { return _paths; }
    NLPathReadFunction getRead() const { return _read; }
    Column* getOutput() const { return _output; }
    PathElementsKind getKind() const { return _kind; }
    QueryListBuffer* getListBuffer() const { return _listBuffer; }

private:
    const Column* _paths {nullptr};
    NLPathReadFunction _read {nullptr};
    Column* _output {nullptr};
    PathElementsKind _kind {PathElementsKind::Nodes};
    QueryListBuffer* _listBuffer {nullptr};
};

// One step of a path being built, over the columns that hold it on each row. Start opens
// the path on a node, Hop adds an edge and the node it lands on, Walk adds the hops of a
// walk taken with the pattern, and ReversedWalk the hops of one taken against it, closing
// on the node it seeded from.
struct NLPathStep {
    enum class Kind : uint8_t {
        Start,
        Hop,
        Walk,
        ReversedWalk,
    };

    Kind _kind {Kind::Start};
    const ColumnNodeIDs* _nodes {nullptr};
    const ColumnEdgeIDs* _edges {nullptr};
    const ColumnVector<PathRef>* _walks {nullptr};
};

// nl.make_path data: one path per row, built by running its steps in order
class NLMakePathData : public NLFunctionData {
public:
    using PathSteps = std::vector<NLPathStep>;

    NLMakePathData(ColumnVector<GraphPath>* output, const PathTrie* trie)
        : _output(output),
        _trie(trie)
    {
    }

    const PathSteps& steps() const { return _steps; }
    ColumnVector<GraphPath>* getOutput() const { return _output; }
    const PathTrie* getTrie() const { return _trie; }

    void addStep(const NLPathStep& step) {
        _steps.push_back(step);
    }

private:
    PathSteps _steps;
    ColumnVector<GraphPath>* _output {nullptr};
    const PathTrie* _trie {nullptr};
};

template <typename T>
struct DjistrakaNode {
    NodeID id;
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <typename T>
struct HeapMapValues {
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <typename T>
struct DjistrakaNodeComparator {
    bool operator()(const DjistrakaNode<T> l, const DjistrakaNode<T> r) const {
        return l.distance > r.distance;
    }
};

class NLShortestPathState {
public:
    NLShortestPathState(ValueType valueType, PropertyTypeID propertyType)
        : _valueType(valueType),
        _propertyType(propertyType)
    {
    }

    virtual ~NLShortestPathState();

    ValueType getValueType() const { return _valueType; }
    PropertyTypeID getPropertyType() const { return _propertyType; }

    std::unordered_set<NodeID>& targets() { return _targetNodes; }

    // Reset the whole accumulator, and seed the frontier with a producing step's source nodes
    // (each at distance zero). Typed on the weight, so implemented by the templated impl; the
    // drain reaches the typed frontier through the impl's accessors.
    virtual void reset() = 0;
    virtual void addSources(const std::vector<NodeID>& nodes) = 0;

protected:
    ValueType _valueType {ValueType::Invalid};
    PropertyTypeID _propertyType;
    std::unordered_set<NodeID> _targetNodes;
};

template <SupportedType T>
class NLShortestPathStateImpl final : public NLShortestPathState {
public:
    using EdgePropType = typename T::Primitive;
    using DjistrakaHeap = std::priority_queue<DjistrakaNode<EdgePropType>,
                                              std::vector<DjistrakaNode<EdgePropType>>,
                                              DjistrakaNodeComparator<EdgePropType>>;
    using DjistrakaValueMap = std::unordered_map<NodeID, HeapMapValues<EdgePropType>>;

    using NLShortestPathState::NLShortestPathState;

    void reset() override {
        _heap = DjistrakaHeap();
        _heapValueMap.clear();
        _targetNodes.clear();
    }

    void addSources(const std::vector<NodeID>& nodes) override {
        for (const NodeID val : nodes) {
            const auto [slot, inserted] = _heapValueMap.insert({
                val, {NodeID(), EdgeID(), 0}
            });

            if (inserted) {
                _heap.push({val, NodeID(), EdgeID(), 0});
            }
        }
    }

    DjistrakaHeap& heap() { return _heap; }
    DjistrakaValueMap& heapValueMap() { return _heapValueMap; }

private:
    DjistrakaHeap _heap;
    DjistrakaValueMap _heapValueMap;
};

// nl.shortest_path_buffer data: empties the accumulator each time the block it lives
// in runs - once at function scope
class NLNodeSetResetData : public NLFunctionData {
public:
    NLNodeSetResetData(NLNodeSetState* state)
        : _state(state)
    {
    }

    NLNodeSetState* getState() const { return _state; }

private:
    NLNodeSetState* _state {nullptr};
};

class NLNodeSetCollectData : public NLFunctionData {
public:
    NLNodeSetCollectData(NLNodeSetState* state, const ColumnNodeIDs* nodes)
        : _state(state),
        _nodes(nodes)
    {
    }

    NLNodeSetState* getState() const { return _state; }
    const ColumnNodeIDs* getNodes() const { return _nodes; }

private:
    NLNodeSetState* _state {nullptr};
    const ColumnNodeIDs* _nodes {nullptr};
};

class NLShortestPathResetData : public NLFunctionData {
public:
    NLShortestPathResetData(NLShortestPathState* state)
        : _state(state)
    {
    }

    NLShortestPathState* getState() const { return _state; }

private:
    NLShortestPathState* _state {nullptr};
};

class NLShortestPathUpdateData : public NLFunctionData {
public:
    NLShortestPathUpdateData(NLShortestPathState* state, const ColumnNodeIDs* nodes, bool target)
        : _state(state),
        _nodes(nodes),
        _target(target)
    {
    }

    NLShortestPathState* getState() const { return _state; }
    const ColumnNodeIDs* getNodes() const { return _nodes; }
    bool isTarget() const { return _target; }

private:
    NLShortestPathState* _state {nullptr};
    const ColumnNodeIDs* _nodes {nullptr};
    bool _target {false};
};

class NLShortestPathLoopData : public NLFunctionData {
public:
    NLShortestPathLoopData(NLShortestPathState* state)
        : _state(state)
    {
    }

    NLShortestPathState* getState() const { return _state; }

    void setDistanceOutput(Column* distance) { _distance = distance; }
    void setPathOutput(ColumnVector<GraphPath>* path) { _path = path; }

    Column* getDistanceOutput() const { return _distance; }
    ColumnVector<GraphPath>* getPathOutput() const { return _path; }

    // The scratch the search reuses to expand one popped node's out-edges and read their
    // weights, allocated once at translation. The edge/node/weight columns are LocalMemory
    // columns; the index column is plain scratch, as on NLEdgeLoopData / NLSortLoopData.
    void setExpansionScratch(ColumnNodeIDs* input,
                             ColumnEdgeIDs* edges,
                             ColumnNodeIDs* targets,
                             Column* weights) {
        _expansionInput = input;
        _expandedEdges = edges;
        _expandedTargets = targets;
        _weightValues = weights;
    }

    ColumnNodeIDs* getExpansionInput() const { return _expansionInput; }
    ColumnEdgeIDs* getExpandedEdges() const { return _expandedEdges; }
    ColumnNodeIDs* getExpandedTargets() const { return _expandedTargets; }
    Column* getWeightValues() const { return _weightValues; }
    ColumnVector<size_t>* getExpandedIndices() { return &_expandedIndices; }

    NLStmtContainer* getStmts() { return &_stmts; }
    const NLStmtContainer* getStmts() const { return &_stmts; }

private:
    NLShortestPathState* _state {nullptr};

    Column* _distance {nullptr};
    ColumnVector<GraphPath>* _path {nullptr};

    ColumnNodeIDs* _expansionInput {nullptr};
    ColumnEdgeIDs* _expandedEdges {nullptr};
    ColumnNodeIDs* _expandedTargets {nullptr};
    Column* _weightValues {nullptr};
    ColumnVector<size_t> _expandedIndices;

    NLStmtContainer _stmts;
};

}
