#include "BFSExpandInEdgesProcessor.h"

#include <algorithm>

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "LocalMemory.h"

#include "iterators/GetInEdgesIterator.h"

#include "dataframe/NamedColumn.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnIndices.h"

#include "EntityList.h"
#include "PipelineBuffer.h"

using namespace db;

BFSExpandInEdgesProcessor::BFSExpandInEdgesProcessor(LocalMemory* mem,
                                                     int64_t minHops,
                                                     int64_t maxHops)
    : _mem(mem),
      _minHops(minHops),
      _maxHops(maxHops) {
}

BFSExpandInEdgesProcessor::~BFSExpandInEdgesProcessor() {
}

std::string BFSExpandInEdgesProcessor::describe() const {
    return fmt::format("BFSExpandInEdgesProcessor @={} hops=[{},{}]",
                       fmt::ptr(this), _minHops, _maxHops);
}

BFSExpandInEdgesProcessor* BFSExpandInEdgesProcessor::create(
    PipelineV2* pipeline,
    LocalMemory* mem,
    int64_t minHops,
    int64_t maxHops) {
    auto* proc = new BFSExpandInEdgesProcessor(mem, minHops, maxHops);

    PipelineInputPort* inPort = PipelineInputPort::create(pipeline, proc);
    PipelineOutputPort* outPort = PipelineOutputPort::create(pipeline, proc);

    proc->_input.setPort(inPort);
    proc->_output.setPort(outPort);

    proc->addInput(inPort);
    proc->addOutput(outPort);

    proc->postCreate(pipeline);

    return proc;
}

void BFSExpandInEdgesProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    const GraphView& view = ctxt->getGraphView();

    // Input
    _inputTargets = dynamic_cast<ColumnNodeIDs*>(_input.getNodeIDs()->getColumn());
    bioassert(_inputTargets, "Input target nodes column is null");

    // Output
    bioassert(_outputSources, "Output sources nodes column is null");
    bioassert(_outputPaths, "Output paths column is null");
    bioassert(_outputIndices, "Output indices column is null");
    bioassert(dynamic_cast<ColumnVector<EntityList>*>(_outputPaths->getColumn()), "Output paths column is not a path column");
    bioassert(dynamic_cast<ColumnNodeIDs*>(_outputSources->getColumn()), "Output sources column is not an entity ID column");

    // BFS Execution state
    _bfsTargets = _mem->alloc<ColumnNodeIDs>();
    _bfsEdges = _mem->alloc<ColumnEdgeIDs>();
    _bfsIntermediates = _mem->alloc<ColumnNodeIDs>();
    _bfsIndices = _mem->alloc<ColumnIndices>();

    _bfsWriter = std::make_unique<GetInEdgesChunkWriter>(view, _bfsTargets);
    _bfsWriter->setIndices(_bfsIndices);
    _bfsWriter->setEdgeIDs(_bfsEdges);
    _bfsWriter->setSrcIDs(_bfsIntermediates);

    markAsPrepared();
}

void BFSExpandInEdgesProcessor::reset() {
    markAsReset();
}

void BFSExpandInEdgesProcessor::execute() {
    _outputIndices->clear();

    auto* outputSources = static_cast<ColumnNodeIDs*>(_outputSources->getColumn());
    auto* outputPaths = static_cast<ColumnVector<EntityList>*>(_outputPaths->getColumn());

    outputPaths->clear();

    struct FrontierEntry {
        NodeID node;
        EntityList edges;
        // bool _finished {false}; // TODO: Add this to the frontier
        //       and use it to avoid reexploring over and over again
    };

    const size_t inputSize = _inputTargets->size();
    const ColumnNodeIDs& inputNodes = *_inputTargets;
    std::vector<FrontierEntry> frontier;
    std::vector<FrontierEntry> nextFrontier;

    ColumnNodeIDs& bfsTargets = *_bfsTargets;
    ColumnEdgeIDs& bfsEdges = *_bfsEdges;
    ColumnNodeIDs& bfsIntermediates = *_bfsIntermediates;
    ColumnIndices& bfsIndices = *_bfsIndices;

    for (size_t targetIdx = 0; targetIdx < inputSize; targetIdx++) {
        const NodeID targetNode = inputNodes[targetIdx];
        frontier.resize(1);
        frontier[0].node = targetNode;
        frontier[0].edges = {};

        for (int64_t depth = 1; !frontier.empty(); depth++) {
            if (_maxHops != std::numeric_limits<int64_t>::max() && depth > _maxHops) {
                break;
            }

            // Prepare new set of targets
            bfsTargets.resize(frontier.size());
            for (size_t i = 0; i < frontier.size(); i++) {
                bfsTargets[i] = frontier[i].node;
            }

            bfsEdges.clear();
            bfsIntermediates.clear();
            bfsIndices.clear();

            _bfsWriter->reset();
            _bfsWriter->fill(SIZE_MAX); // TODO -> Make the bfs processor chunked

            EntityList::Entry edgeVal {
                ._type = EntityType::Edge,
            };

            nextFrontier.clear();
            for (size_t i = 0; i < bfsEdges.size(); i++) {
                const NodeID intermediate = bfsIntermediates[i];
                const size_t parentIdx = bfsIndices[i];
                edgeVal._id = bfsEdges[i].getValue();

                const EntityList& parentPath = frontier[parentIdx].edges;
                const bool alreadyUsed = std::find(
                                             parentPath.begin(), parentPath.end(),
                                             edgeVal)
                                      != parentPath.end();
                if (alreadyUsed) {
                    continue;
                }

                FrontierEntry& newEntry = nextFrontier.emplace_back();
                newEntry.edges = parentPath; // Copy parent path
                newEntry.edges.add(edgeVal._type, edgeVal._id);
                newEntry.node = intermediate;

                if (depth >= _minHops) {
                    _outputIndices->push_back(targetIdx);
                    outputPaths->push_back(newEntry.edges);
                }
            }

            frontier = std::move(nextFrontier);
        }
    }

    outputSources->resize(frontier.size());
    for (size_t i = 0; i < frontier.size(); i++) {
        outputSources->set(i, frontier[i].node);
    }

    _input.getPort()->consume();
    _output.getPort()->writeData();
    finish();
}
