#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <limits>
#include <span>
#include <type_traits>
#include <unordered_set>

#include <spdlog/fmt/bundled/format.h>
#include <llvm/Support/MathExtras.h>

#include "ID.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetPropertiesWithNullIterator.h"
#include "iterators/PathDistanceIndex.h"
#include "iterators/PathExplorator.h"
#include "iterators/PathTargetIndex.h"
#include "iterators/PathHopFilter.h"
#include "iterators/PathLabelHopFilter.h"
#include "iterators/ReversedPathExplorator.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "CellOrder.h"
#include "QueryTypeName.h"
#include "list/PathTrie.h"
#include "metadata/PropertyType.h"

#include "versioning/CommitWriteBuffer.h"
#include "versioning/PendingAdjacency.h"
#include "views/GraphView.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLWrittenValues.h"
#include "NLColumnKernels.h"
#include "NLLoopSteps.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

template <typename IDType>
ListView listPathEntities(const PathView path,
                          size_t count,
                          IDType (PathView::*readEntity)(size_t) const,
                          CellTag tag,
                          QueryListBuffer& buffer) {
    ListWriteCursor cursor = buffer.reserveList(count, count * sizeof(IDType));

    for (size_t index = 0; index < count; index++) {
        cursor.writeValueAt(index, tag, (path.*readEntity)(index));
    }

    return cursor.getView();
}

std::optional<PathView> columnPath(const Column* paths, size_t row) {
    const GraphPath& path = (*static_cast<const ColumnVector<GraphPath>*>(paths))[row];
    if (path.isNull()) {
        return std::nullopt;
    }

    return PathView {path.getIDs()};
}

// The path a tagged cell holds, or nothing where it holds a null. Anything else is a path
// function applied to something that is no path, which only the cell's tag tells per row.
std::optional<PathView> taggedPath(const CellView cell) {
    const CellTag tag = cell.getTag();

    if (tag == CellTag::Path) {
        return cell.getAs<PathView>();
    } else if (tag == CellTag::Null) {
        return std::nullopt;
    }

    throw IRException(fmt::format("nodes(), relationships() and length() read a path, not '{}'", queryTypeName(tag)));
}

std::optional<PathView> taggedColumnPath(const Column* paths, size_t row) {
    return taggedPath((*static_cast<const ColumnVector<CellView>*>(paths))[row]);
}

void readPathElements(const Column* paths,
                      NLPathReadFunction read,
                      PathElementsKind kind,
                      Column* output,
                      QueryListBuffer& listBuffer) {
    const size_t rowCount = paths->size();

    if (kind == PathElementsKind::Length) {
        ColumnOptVector<uint64_t>* lengths = static_cast<ColumnOptVector<uint64_t>*>(output);
        resetForRowWrites(lengths, rowCount);

        for (size_t row = 0; row < rowCount; row++) {
            const std::optional<PathView> path = read(paths, row);
            if (!path) {
                lengths->setNull(row);
            } else {
                lengths->set(row, path->getLength());
            }
        }

        return;
    }

    ColumnOptVector<ListView>* lists = static_cast<ColumnOptVector<ListView>*>(output);
    resetForRowWrites(lists, rowCount);

    for (size_t row = 0; row < rowCount; row++) {
        const std::optional<PathView> path = read(paths, row);
        if (!path) {
            lists->setNull(row);
        } else if (kind == PathElementsKind::Nodes) {
            lists->set(row, listPathEntities(*path, path->getNodeCount(), &PathView::getNode, CellTag::NodeID, listBuffer));
        } else {
            lists->set(row, listPathEntities(*path, path->getLength(), &PathView::getEdge, CellTag::EdgeID, listBuffer));
        }
    }
}

template <SupportedType T>
void shortestPathSearch(NLExecutionContext* context, NLShortestPathLoopData* loopData) {
    using EdgePropType = typename T::Primitive;

    const GraphView& view = *context->getView();
    NLShortestPathStateImpl<T>* state = static_cast<NLShortestPathStateImpl<T>*>(loopData->getState());

    ColumnNodeIDs* input = loopData->getExpansionInput();
    ColumnEdgeIDs* outputEdges = loopData->getExpandedEdges();
    ColumnNodeIDs* outputNodes = loopData->getExpandedTargets();
    ColumnVector<size_t>* outputIndices = loopData->getExpandedIndices();
    ColumnOptVector<EdgePropType>* properties =
        static_cast<ColumnOptVector<EdgePropType>*>(loopData->getWeightValues());

    GetOutEdgesChunkWriter getOutEdgesWriter(view, input);
    getOutEdgesWriter.setIndices(outputIndices);
    getOutEdgesWriter.setEdgeIDs(outputEdges);
    getOutEdgesWriter.setTgtIDs(outputNodes);

    const PropertyTypeID weightType = state->getPropertyType();
    GetPropertiesWithNullChunkWriter<EdgeID, T> getPropertiesWriter(view, weightType, outputEdges);
    getPropertiesWriter.setOutput(properties);

    CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    NLWrittenValues& written = context->getWrittenValues();
    if (writeBuffer) {
        written.indexUpdates(writeBuffer);
    }

    const bool hasUpdates = writeBuffer && written.hasUpdates();
    NLValueStore weightValues;

    const std::unordered_set<NodeID>& targetNodes = state->targets();

    typename NLShortestPathStateImpl<T>::DjistrakaHeap& heap = state->heap();
    typename NLShortestPathStateImpl<T>::DjistrakaValueMap& heapValueMap = state->heapValueMap();

    while (!heap.empty()) {
        const DjistrakaNode<EdgePropType> val = heap.top();

        if (targetNodes.contains(val.id)) {
            // target found
            break;
        }

        heap.pop();

        // We only need to confirm if the distance value is stale
        // as we only add a new value to the heap when we find a shorter
        // path to a node.
        const auto it = heapValueMap.find(val.id);
        if (it != heapValueMap.end() && it->second.distance != val.distance) {
            // remove stale value
            continue;
        }

        // Set up the input columns and iterators
        input->clear();
        input->push_back(val.id);
        getOutEdgesWriter.reset();
        // We set the chunk size to SIZE_MAX so that we expand the iterators
        // completely in 1 chunk
        getOutEdgesWriter.fill(std::numeric_limits<size_t>::max());

        getPropertiesWriter.reset();
        getPropertiesWriter.fill(std::numeric_limits<size_t>::max());

        if (hasUpdates) {
            for (size_t row = 0; row < properties->size(); row++) {
                const CommitWriteBuffer::SupportedTypeVariant* update = written.findEdgeUpdate((*outputEdges)[row], weightType);
                if (update) {
                    properties->setOptional(row, written.read<T>(*update, weightValues));
                }
            }

            properties->dropAllValidMask();
        }

        // loop over all the edge properties
        for (size_t i = 0; i < properties->size(); ++i) {
            if (!properties->isValid(i)) {
                continue;
            }

            const EdgePropType& weight = properties->getValue(i);

            if constexpr (std::is_signed_v<EdgePropType>) {
                if (weight < 0) {
                    throw IRException("Cannot Do Shortest Path With Negative Weights");
                }
            }

            const auto outputNodeId = (*outputNodes)[i];
            const auto outputEdgeId = (*outputEdges)[i];
            const auto dist = val.distance + weight;

            const auto it = heapValueMap.find(outputNodeId);
            if (it == heapValueMap.end()) {
                // If the node is not in the heapValueMap
                // then it is an unexplored node so we add it into heap
                // directly
                heap.push({outputNodeId, val.id, outputEdgeId, dist});
                heapValueMap[outputNodeId] = {val.id, outputEdgeId, dist};
            } else {
                // If we have come across this nodeID before we check if the new path
                // to the node is shorter than the current shortest path for the node
                // If so we replace it by pushing the new djistraka node into the heap
                // and changing the heapValueMap value to match the new shortest distance.
                //
                // The old path's node will still exist in the heap but if we ever pop
                // it from the heap we can compare it to the latest value in the heapValueMap
                // to invalidate it.
                if (dist < it->second.distance) {
                    heap.push({outputNodeId, val.id, outputEdgeId, dist});
                    heapValueMap[outputNodeId] = {val.id, outputEdgeId, dist};
                }
            }
        }
    }

    // Once we are out of the while loop if there are no values in the heap that means
    // no path was found between the source and target sets.

    if (heap.empty()) {
        return;
    }

    // After the loop the top value in the heap is the target node with the shortest
    // distance from the source set.

    Column* dist = loopData->getDistanceOutput();
    auto* distCol = static_cast<ColumnVector<EdgePropType>*>(dist);
    ColumnVector<GraphPath>* pathCol = loopData->getPathOutput();

    distCol->clear();
    pathCol->clear();

    // Get the total length of the path
    distCol->push_back(heap.top().distance);
    GraphPath& path = pathCol->emplace_back();

    // In the heap all the (non-stale) values represent the shortest path to that
    // nodeID from the source set. So we can find the shortet path by following the
    // previousNode back from the target node that we have found.
    auto lastNode = heap.top().prevNode;
    auto edge = heap.top().edge;
    path.start(heap.top().id);

    while (lastNode.isValid()) {
        path.addHop(edge, lastNode);

        const auto& pathInfo = heapValueMap[lastNode];
        lastNode = pathInfo.prevNode;
        edge = pathInfo.edge;
    }

    std::ranges::reverse(path.getIDs());

    runBody(context, loopData->getStmts());
}

// Runs the hop predicate of one step of an nl.explore_paths over a batch of frames: the
// candidates and the frame's repetition so far are copied a chunk at a time into the columns
// the hop statements read, and the survivors are compacted to the front of the batch. A chunk
// may start and end mid-frame. The hop_labels filter, when there is one, cuts the batch first.
class NLHopFilter : public PathHopFilter {
public:
    NLHopFilter(NLExecutionContext* context, const NLExploreStep* step, PathHopFilter* labelFilter)
        : _context(context),
        _step(step),
        _labelFilter(labelFilter)
    {
        _indices.reserve(context->getChunkSize());
    }

    ~NLHopFilter() override {
    }

    size_t filter(std::span<PathHopFrame> frames, std::span<NodeID> candidateNodes, std::span<EdgeID> candidateEdges) override {
        if (_labelFilter) {
            const size_t labelled = _labelFilter->filter(frames, candidateNodes, candidateEdges);
            candidateNodes = candidateNodes.first(labelled);
            candidateEdges = candidateEdges.first(labelled);
        }

        const size_t chunkSize = _context->getChunkSize();
        const std::span<ColumnNodeIDs* const> nodes = _step->getHopNodes();
        const std::span<ColumnEdgeIDs* const> edges = _step->getHopEdges();
        const size_t position = _step->getHopPosition();
        ColumnNodeIDs* sources = nodes[position];
        ColumnEdgeIDs* hopEdges = edges[position];
        ColumnNodeIDs* ends = nodes[position + 1];
        const Column* mask = _step->getHopMask();
        const NLMaskSurvivorFunction survivors = _step->getHopSurvivors();
        const NLStmtContainer* stmts = _step->getHopStmts();
        const std::span<const NLHopImport> imports = _step->getHopImports();

        _frameEnds.clear();
        size_t frameEnd = 0;
        for (PathHopFrame& frame : frames) {
            frameEnd += frame._candidateCount;
            _frameEnds.push_back(frameEnd);
            frame._candidateCount = 0;
        }

        std::vector<size_t>& seedRows = _seedRows.getRaw();
        size_t fillFrame = 0;
        size_t survivorFrame = 0;
        size_t kept = 0;
        for (size_t begin = 0; begin < candidateNodes.size(); begin += chunkSize) {
            const size_t count = std::min(chunkSize, candidateNodes.size() - begin);
            const size_t end = begin + count;

            sources->resize(count);
            seedRows.resize(count);
            for (size_t earlier = 0; earlier < position; earlier++) {
                if (nodes[earlier]) {
                    nodes[earlier]->resize(count);
                }

                if (edges[earlier]) {
                    edges[earlier]->resize(count);
                }
            }

            for (size_t segmentBegin = begin; segmentBegin < end;) {
                const size_t segmentEnd = std::min(end, _frameEnds[fillFrame]);
                const PathHopFrame& frame = frames[fillFrame];
                const size_t segmentOffset = segmentBegin - begin;
                const size_t segmentSize = segmentEnd - segmentBegin;
                std::fill_n(sources->begin() + segmentOffset, segmentSize, frame._source);
                std::fill_n(seedRows.begin() + segmentOffset, segmentSize, frame._seedRow);

                for (size_t earlier = 0; earlier < position; earlier++) {
                    if (nodes[earlier]) {
                        std::fill_n(nodes[earlier]->begin() + segmentOffset, segmentSize, frame._repetitionNodes[earlier]);
                    }

                    if (edges[earlier]) {
                        std::fill_n(edges[earlier]->begin() + segmentOffset, segmentSize, frame._repetitionEdges[earlier]);
                    }
                }

                if (segmentEnd == _frameEnds[fillFrame]) {
                    fillFrame++;
                }
                segmentBegin = segmentEnd;
            }

            hopEdges->resize(count);
            std::copy_n(candidateEdges.begin() + begin, count, hopEdges->begin());

            ends->resize(count);
            std::copy_n(candidateNodes.begin() + begin, count, ends->begin());

            for (const NLHopImport& import : imports) {
                import._gather(import._source, &_seedRows, import._chunk);
            }

            runBody(_context, stmts);

            _indices.getRaw().clear();
            survivors(mask, &_indices);

            for (const size_t survivor : _indices.getRaw()) {
                const size_t candidatePosition = begin + survivor;
                while (candidatePosition >= _frameEnds[survivorFrame]) {
                    survivorFrame++;
                }

                frames[survivorFrame]._candidateCount++;
                candidateNodes[kept] = candidateNodes[candidatePosition];
                candidateEdges[kept] = candidateEdges[candidatePosition];
                kept++;
            }
        }

        return kept;
    }

    bool readsRepetition() const override {
        const std::span<ColumnNodeIDs* const> nodes = _step->getHopNodes();
        const std::span<ColumnEdgeIDs* const> edges = _step->getHopEdges();
        const size_t position = _step->getHopPosition();

        const auto isRead = [](const Column* column) { return column != nullptr; };

        return std::any_of(nodes.begin(), nodes.begin() + position, isRead)
            || std::any_of(edges.begin(), edges.begin() + position, isRead);
    }

private:
    NLExecutionContext* _context {nullptr};
    const NLExploreStep* _step {nullptr};
    PathHopFilter* _labelFilter {nullptr};
    ColumnVector<size_t> _indices;
    ColumnVector<size_t> _seedRows;
    std::vector<size_t> _frameEnds;
};

// The direction and edge types every step of the body follows together: what a walk can reach
// over them bounds what the body reaches, so the pruning indexes are built over them
PathExplorationDir combinedDirection(const NLExplorePathsLoopData* loopData) {
    const NLExploreStep* firstStep = loopData->getStep(0);
    const PathExplorationDir first = firstStep->getDirection();
    const size_t stepCount = loopData->getStepCount();

    for (size_t step = 1; step < stepCount; step++) {
        const NLExploreStep* exploreStep = loopData->getStep(step);
        if (exploreStep->getDirection() != first) {
            return PathExplorationDir::BOTH;
        }
    }

    return first;
}

// Empty when a step follows every type
void combinedEdgeTypes(const NLExplorePathsLoopData* loopData, std::vector<EdgeTypeID>& edgeTypes) {
    edgeTypes.clear();

    const size_t stepCount = loopData->getStepCount();
    for (size_t step = 0; step < stepCount; step++) {
        const NLExploreStep* exploreStep = loopData->getStep(step);
        if (!exploreStep->filtersByType()) {
            edgeTypes.clear();
            return;
        }

        const std::span<const EdgeTypeID> stepTypes = exploreStep->getEdgeTypes();
        edgeTypes.insert(edgeTypes.end(), stepTypes.begin(), stepTypes.end());
    }

    std::sort(edgeTypes.begin(), edgeTypes.end());
    edgeTypes.erase(std::unique(edgeTypes.begin(), edgeTypes.end()), edgeTypes.end());
}

// The hop bounds of the loop count repetitions of the body, the walk's count its edges
uint64_t edgesOfRepetitions(uint64_t repetitions, size_t stepCount) {
    return llvm::SaturatingMultiply<uint64_t>(repetitions, stepCount);
}

// The body the walk repeats, as a sample expands it. A filter reading the repetition sees
// none in the sample, so its step is sampled unfiltered.
void sampledBodyOf(std::span<const PathStep> walkedBody, std::vector<PathStep>& body) {
    body.assign(walkedBody.begin(), walkedBody.end());
    for (PathStep& bodyStep : body) {
        const bool readsRepetition = bodyStep._hopFilter && bodyStep._hopFilter->readsRepetition();
        if (readsRepetition) {
            bodyStep._hopFilter = nullptr;
        }
    }
}

// The distance index of an end-constrained exploration is built at most once per loop, the
// first time the seeds seen over all its chunks make the enumeration costlier than the
// index; null until then
const PathDistanceIndex* pruningIndexFor(const GraphView& view,
                                         NLExplorePathsLoopData* loopData,
                                         uint64_t maxHops,
                                         const PathDistanceIndex::SeedExpansion& expansion) {
    PathDistanceIndex* index = loopData->getDistanceIndex();
    if (index->isBuilt()) {
        return index;
    }

    std::vector<EdgeTypeID> edgeTypes;
    combinedEdgeTypes(loopData, edgeTypes);

    const PathExplorationDir direction = combinedDirection(loopData);
    const PartDirectory parts(view);
    const double walkChecks = PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, loopData->getSeedsSeen(), maxHops);
    const bool built = index->buildWithin(view, loopData->getEndLabels(), direction, edgeTypes, maxHops, walkChecks);

    return built ? index : nullptr;
}

// The target index of an end set is the whole walk's: one search from every node of the set,
// built once the seeds seen imply more enumeration than that search costs
const PathTargetIndex* endSetTargetIndexFor(const GraphView& view,
                                            NLExplorePathsLoopData* loopData,
                                            std::span<const NodeID> endNodes,
                                            uint64_t maxHops,
                                            const PathDistanceIndex::SeedExpansion& expansion) {
    PathTargetIndex* index = loopData->getTargetIndex();
    if (index->isBuilt()) {
        return index;
    }

    std::vector<EdgeTypeID> edgeTypes;
    combinedEdgeTypes(loopData, edgeTypes);

    const PathExplorationDir direction = combinedDirection(loopData);
    const bool worthBuilding = PathTargetIndex::isWorthBuildingSet(view, direction, edgeTypes, expansion, loopData->getSeedsSeen(), endNodes.size(), maxHops);
    if (!worthBuilding) {
        return nullptr;
    }

    index->buildSet(view, endNodes, direction, edgeTypes, maxHops);

    return index;
}

// The target index of a bound end is a chunk's own: its distinct targets, batched 64 per
// word, when the enumeration they imply is costlier than the batches; null otherwise
const PathTargetIndex* targetIndexFor(const GraphView& view,
                                      NLExplorePathsLoopData* loopData,
                                      uint64_t maxHops,
                                      const PathDistanceIndex::SeedExpansion& expansion) {
    const std::vector<NodeID>& endNodes = loopData->getEndNodes()->getRaw();

    std::vector<NodeID> targets(endNodes.begin(), endNodes.end());
    std::sort(targets.begin(), targets.end());
    targets.erase(std::unique(targets.begin(), targets.end()), targets.end());

    std::vector<EdgeTypeID> edgeTypes;
    combinedEdgeTypes(loopData, edgeTypes);

    const PathExplorationDir direction = combinedDirection(loopData);
    const bool worthBuilding = PathTargetIndex::isWorthBuilding(view, direction, edgeTypes, expansion, endNodes.size(), targets.size(), maxHops);
    if (!worthBuilding) {
        return nullptr;
    }

    PathTargetIndex* index = loopData->getTargetIndex();
    index->build(view, targets, direction, edgeTypes, maxHops);

    return index;
}

// A walk to an end set emits the same rows from either end when no path is read and no hop
// test sees which way the hop was taken
bool emitsTheSameFromEitherEnd(const NLExplorePathsLoopData* loopData, std::span<const PathStep> walkedBody) {
    const bool hasHopFilter = std::ranges::any_of(walkedBody, [](const PathStep& step) { return step._hopFilter != nullptr; });

    return !loopData->getPaths() && !hasHopFilter;
}

void runFromTheEndSet(NLExecutionContext* context,
                      NLExplorePathsLoopData* loopData,
                      ReversedPathExplorator::Body body,
                      std::span<const NodeID> endNodes,
                      uint64_t minHops,
                      uint64_t maxHops,
                      bool walksPendingEdges,
                      const PathDistanceIndex::SeedExpansion& fromEnds) {
    const GraphView& view = *context->getView();
    const LabelSet* endLabels = loopData->filtersByEndLabels() ? &loopData->getEndLabels() : nullptr;

    ReversedPathExplorator explorator(view,
                                      loopData->getInput(),
                                      endNodes,
                                      endLabels,
                                      body,
                                      minHops,
                                      maxHops);
    explorator.setIndices(loopData->getIndices());
    explorator.setTargets(loopData->getTargets());
    const bool distinctEnds = loopData->isDistinctEnds();
    explorator.setDistinctEnds(distinctEnds);

    if (context->getWriteBuffer()) {
        const PendingAdjacency& pendingAdjacency = context->getPendingAdjacency();
        explorator.setPendingAdjacency(&pendingAdjacency, pendingAdjacency.getEdgeIDBound());
    }

    std::vector<EdgeTypeID> edgeTypes;
    combinedEdgeTypes(loopData, edgeTypes);

    // The seeds are what this walk heads for, so they are the set its index is searched from
    PathTargetIndex seedIndex;
    const std::span<const NodeID> seedNodes = explorator.getSeedNodes();
    const PathExplorationDir reversed = reverseOf(combinedDirection(loopData));
    const bool indexesSeeds = !distinctEnds
                           && !walksPendingEdges
                           && PathTargetIndex::isWorthBuildingSet(view, reversed, edgeTypes, fromEnds, endNodes.size(), seedNodes.size(), maxHops);
    if (indexesSeeds) {
        seedIndex.buildSet(view, seedNodes, reversed, edgeTypes, maxHops);
        explorator.setTargetIndex(&seedIndex);
    }

    runEdgeLoopSteps(context, loopData, &explorator, nullptr, loopData->getSources());
}

}

void NLExecutor::runNodeSetReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLNodeSetResetData* reset = static_cast<NLNodeSetResetData*>(data);

    reset->getState()->reset();
}

void NLExecutor::runNodeSetCollect(NLExecutionContext* context, NLFunctionData* data) {
    const NLNodeSetCollectData* collect = static_cast<NLNodeSetCollectData*>(data);

    collect->getState()->add(collect->getNodes()->getRaw());
}

void NLExecutor::runExplorePathsLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLExplorePathsLoopData* loopData = static_cast<NLExplorePathsLoopData*>(data);
    const ColumnNodeIDs* inputNodeIDs = loopData->getInput();

    if (inputNodeIDs->empty()) {
        return;
    }

    // An end label absent from the schema is carried by no node, so no path can end
    const bool filtersByEndLabels = loopData->filtersByEndLabels();
    if (filtersByEndLabels && !loopData->isEndMatchable()) {
        return;
    }

    // The ends a whole set binds, gathered by the loop that closed before this one opened.
    // No node of the set is no node to end on, whatever the hop bounds say.
    NLNodeSetState* const endNodeSet = loopData->getEndNodeSet();
    std::span<const NodeID> endNodes;
    if (endNodeSet) {
        endNodes = endNodeSet->getNodes();
        if (endNodes.empty()) {
            return;
        }
    }

    // An edge type absent from the schema matches no edge, so nothing is ever expanded;
    // the zero-length rows of a min of zero still come out, so this is not an early return
    const size_t stepCount = loopData->getStepCount();
    const uint64_t minHops = edgesOfRepetitions(loopData->getMinHops(), stepCount);
    const uint64_t maxHops = loopData->isMatchable() ? edgesOfRepetitions(loopData->getMaxHops(), stepCount) : 0;

    const GraphView& view = *context->getView();
    PathExplorator explorator(view, inputNodeIDs, minHops, maxHops);
    explorator.setIndices(loopData->getIndices());
    explorator.setTargets(loopData->getTargets());

    if (loopData->getPaths()) {
        explorator.setPaths(loopData->getPaths(), loopData->getTrie());
    }

    const bool distinctEnds = loopData->isDistinctEnds();
    explorator.setDistinctEnds(distinctEnds);

    const CommitWriteBuffer* writeBuffer = context->getWriteBuffer();
    if (writeBuffer) {
        PendingAdjacency& pendingAdjacency = context->getPendingAdjacency();
        pendingAdjacency.index(*writeBuffer,
                               context->getFirstQueryEdge(),
                               committedNodeCount(&view),
                               committedEdgeCount(&view));
        explorator.setPendingAdjacency(&pendingAdjacency, pendingAdjacency.getEdgeIDBound());
    }

    // A change that wrote only nodes opens no path the committed graph does not already
    // hold, so only a pending edge puts the two pruning indexes below out of date.
    const bool walksPendingEdges = writeBuffer && writeBuffer->numPendingEdges() > context->getFirstQueryEdge();

    std::vector<std::optional<NLHopFilter>> hopRegionFilters(stepCount);
    std::vector<std::optional<PathLabelHopFilter>> hopLabelFilters(stepCount);
    std::vector<PathStep> walkedBody(stepCount);
    for (size_t step = 0; step < stepCount; step++) {
        const NLExploreStep* exploreStep = loopData->getStep(step);
        PathStep& bodyStep = walkedBody[step];
        bodyStep._direction = exploreStep->getDirection();

        if (exploreStep->filtersByType()) {
            bodyStep._edgeTypes = exploreStep->getEdgeTypes();
        }

        PathHopFilter* hopFilter = nullptr;
        if (exploreStep->filtersByHopLabels()) {
            const PendingAdjacency* pendingAdjacency = writeBuffer ? &context->getPendingAdjacency() : nullptr;
            hopFilter = &hopLabelFilters[step].emplace(view, exploreStep->getHopLabels(), exploreStep->areHopLabelsMatchable(), pendingAdjacency);
        }

        if (exploreStep->hasHopFilter()) {
            hopFilter = &hopRegionFilters[step].emplace(context, exploreStep, hopFilter);
        }

        bodyStep._hopFilter = hopFilter;
        explorator.addStep(bodyStep);
    }

    // The level search of the distinct mode prunes by no index
    const bool prunes = !explorator.searchesLevels() && !walksPendingEdges && (filtersByEndLabels || loopData->getEndNodes() || endNodeSet);

    const bool gatesDistanceIndex = filtersByEndLabels && !loopData->getDistanceIndex()->isBuilt();
    const bool gatesEndSetIndex = endNodeSet && !loopData->getTargetIndex()->isBuilt();
    const bool gatesBoundEndIndex = loopData->getEndNodes() != nullptr;
    const bool readsSample = gatesDistanceIndex || gatesEndSetIndex || gatesBoundEndIndex;

    // Once the walk from the seeds holds its index over the set, it walks only the prefixes
    // that reach the set, as the walk from the set would toward the seeds
    const bool weighsTheEndSet = gatesEndSetIndex && emitsTheSameFromEitherEnd(loopData, walkedBody);

    // What this chunk's own seeds expand to: the gates price the walk by it, and it is what
    // the walk does rather than what the average node carrying the type does
    std::vector<PathStep> body;
    PathDistanceIndex::SeedExpansion expansion;
    const PartDirectory parts(view);
    if (weighsTheEndSet || (prunes && readsSample)) {
        sampledBodyOf(walkedBody, body);
        PathDistanceIndex::sampleSeedExpansion(parts, body, inputNodeIDs->getRaw(), expansion);
    }

    if (weighsTheEndSet) {
        PathDistanceIndex::SeedExpansion fromEnds;
        if (ReversedPathExplorator::isCheaper(parts, body, expansion, inputNodeIDs->size(), endNodes, maxHops, fromEnds)) {
            runFromTheEndSet(context, loopData, body, endNodes, minHops, maxHops, walksPendingEdges, fromEnds);
            return;
        }
    }

    if (prunes) {
        loopData->addSeedsSeen(inputNodeIDs->size());
    }

    if (filtersByEndLabels) {
        explorator.setEndLabels(&loopData->getEndLabels());
        if (prunes) {
            explorator.setDistanceIndex(pruningIndexFor(view, loopData, maxHops, expansion));
        }
    }

    if (loopData->getEndNodes()) {
        explorator.setEndNodes(loopData->getEndNodes());
        if (prunes) {
            explorator.setTargetIndex(targetIndexFor(view, loopData, maxHops, expansion));
        }
    }

    if (endNodeSet) {
        explorator.setEndNodeSet(endNodes);
        if (prunes) {
            explorator.setTargetIndex(endSetTargetIndexFor(view, loopData, endNodes, maxHops, expansion));
        }
    }

    runEdgeLoopSteps(context, loopData, &explorator, nullptr, loopData->getSources());
}

void NLExecutor::runExpandPath(NLExecutionContext* context, NLFunctionData* data) {
    const NLExpandPathData* expand = static_cast<NLExpandPathData*>(data);
    const std::vector<PathRef>& paths = expand->getPaths()->getRaw();
    std::vector<ListView>& lists = expand->getOutput()->getRaw();
    const PathTrie& trie = *expand->getTrie();
    QueryListBuffer& listBuffer = *expand->getListBuffer();
    listBuffer.clear();

    lists.resize(paths.size());

    const bool reversed = expand->isReversed();
    const PathHopStride hops = expand->getHops();

    switch (expand->getKind()) {
        case PathExpansionKind::Edges:
            for (size_t row = 0; row < paths.size(); row++) {
                lists[row] = trie.expandEdges(paths[row], listBuffer, reversed, hops);
            }
        break;

        case PathExpansionKind::Sources: {
            const ColumnNodeIDs* seeds = expand->getSeeds();
            for (size_t row = 0; row < paths.size(); row++) {
                const NodeID seed = seeds ? (*seeds)[row] : NodeID();
                lists[row] = trie.expandSources(paths[row], seed, listBuffer, reversed, hops);
            }
        }
        break;

        case PathExpansionKind::Ends:
            for (size_t row = 0; row < paths.size(); row++) {
                lists[row] = trie.expandEnds(paths[row], listBuffer, reversed, hops);
            }
        break;

        case PathExpansionKind::Nodes: {
            const std::vector<NodeID>& seeds = expand->getSeeds()->getRaw();
            for (size_t row = 0; row < paths.size(); row++) {
                lists[row] = trie.expandNodes(paths[row], seeds[row], listBuffer, reversed);
            }
        }
        break;
    }
}

void NLExecutor::runPathLength(NLExecutionContext* context, NLFunctionData* data) {
    const NLPathLengthData* length = static_cast<NLPathLengthData*>(data);
    const std::vector<PathRef>& paths = length->getPaths()->getRaw();
    std::vector<uint64_t>& lengths = length->getOutput()->getRaw();
    const PathTrie& trie = *length->getTrie();
    const size_t steps = length->getSteps();
    const uint64_t seed = length->countsNodes() ? 1 : 0;

    lengths.resize(paths.size());
    for (size_t row = 0; row < paths.size(); row++) {
        lengths[row] = trie.getDepth(paths[row]) / steps + seed;
    }
}

void NLExecutor::runPathElements(NLExecutionContext* context, NLFunctionData* data) {
    const NLPathElementsData* elements = static_cast<NLPathElementsData*>(data);
    const Column* paths = elements->getPaths();
    const PathElementsKind kind = elements->getKind();
    Column* output = elements->getOutput();
    QueryListBuffer& listBuffer = *elements->getListBuffer();
    listBuffer.clear();

    readPathElements(paths, elements->getRead(), kind, output, listBuffer);
}

void NLExecutor::runMakePath(NLExecutionContext* context, NLFunctionData* data) {
    const NLMakePathData* make = static_cast<NLMakePathData*>(data);
    const NLMakePathData::PathSteps& steps = make->steps();
    std::vector<GraphPath>& paths = make->getOutput()->getRaw();
    const PathTrie& trie = *make->getTrie();

    bioassert(!steps.empty(), "nl.make_path builds a path out of at least one node");

    const NLPathStep& firstStep = steps.front();
    const size_t rowCount = firstStep._nodes ? firstStep._nodes->size() : firstStep._walks->size();

    const auto isRowAligned = [rowCount](const NLPathStep& step) {
        const bool nodesAligned = !step._nodes || step._nodes->size() == rowCount;
        const bool edgesAligned = !step._edges || step._edges->size() == rowCount;
        const bool walksAligned = !step._walks || step._walks->size() == rowCount;

        return nodesAligned && edgesAligned && walksAligned;
    };
    bioassert(std::ranges::all_of(steps, isRowAligned),
              "Entity columns of a path build are not row-aligned.");

    paths.resize(rowCount);
    for (GraphPath& path : paths) {
        path.clear();
    }

    for (const NLPathStep& step : steps) {
        switch (step._kind) {
            case NLPathStep::Kind::Start: {
                const std::vector<NodeID>& nodes = step._nodes->getRaw();
                for (size_t row = 0; row < rowCount; row++) {
                    paths[row].start(nodes[row]);
                }
            }
            break;

            case NLPathStep::Kind::Hop: {
                const std::vector<EdgeID>& edges = step._edges->getRaw();
                const std::vector<NodeID>& nodes = step._nodes->getRaw();
                for (size_t row = 0; row < rowCount; row++) {
                    paths[row].addHop(edges[row], nodes[row]);
                }
            }
            break;

            case NLPathStep::Kind::Walk: {
                const std::vector<PathRef>& walks = step._walks->getRaw();
                for (size_t row = 0; row < rowCount; row++) {
                    trie.appendHops(walks[row], paths[row]);
                }
            }
            break;

            case NLPathStep::Kind::ReversedWalk: {
                const std::vector<PathRef>& walks = step._walks->getRaw();
                const std::vector<NodeID>& seeds = step._nodes->getRaw();
                for (size_t row = 0; row < rowCount; row++) {
                    trie.appendReversedHops(walks[row], seeds[row], paths[row]);
                }
            }
            break;
        }
    }
}

NLPathReadFunction NLExecutor::selectPathRead(const Column* paths) {
    const ColumnKind::Code kind = paths->getKind();

    if (kind == ColumnVector<GraphPath>::staticKind()) {
        return &columnPath;
    } else if (kind == ColumnVector<CellView>::staticKind()) {
        return &taggedColumnPath;
    }

    throw IRException("nodes(), relationships() and length() read a path column, or a column of tagged cells");
}

void NLExecutor::runShortestPathReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLShortestPathResetData* reset = static_cast<NLShortestPathResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runShortestPathUpdate(NLExecutionContext* context, NLFunctionData* data) {
    NLShortestPathUpdateData* update = static_cast<NLShortestPathUpdateData*>(data);
    NLShortestPathState* state = update->getState();

    const std::vector<NodeID>& nodes = update->getNodes()->getRaw();

    // A target only needs to be a member; a source seeds the frontier at distance zero, so it
    // goes through the typed accumulator.
    if (update->isTarget()) {
        state->targets().insert(nodes.begin(), nodes.end());
    } else {
        state->addSources(nodes);
    }
}

void NLExecutor::runShortestPathLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLShortestPathLoopData* loopData = static_cast<NLShortestPathLoopData*>(data);

    switch (loopData->getState()->getValueType()) {
        case ValueType::Double:
            return shortestPathSearch<types::Double>(context, loopData);
        break;

        case ValueType::Int64:
            return shortestPathSearch<types::Int64>(context, loopData);
        break;

        case ValueType::UInt64:
            return shortestPathSearch<types::UInt64>(context, loopData);
        break;

        default:
            throw IRException("SHORTESTPATH weight must be an Int64, UInt64 or Double property");
        break;
    }
}
