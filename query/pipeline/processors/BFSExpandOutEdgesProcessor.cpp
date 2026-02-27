#include "BFSExpandOutEdgesProcessor.h"

#include <algorithm>

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "LocalMemory.h"

#include "iterators/GetOutEdgesIterator.h"

#include "dataframe/NamedColumn.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnIndices.h"

#include "GraphPath.h"
#include "PipelineBuffer.h"

using namespace db;

BFSExpandOutEdgesProcessor::BFSExpandOutEdgesProcessor(LocalMemory* mem,
                                                     int64_t minHops,
                                                     int64_t maxHops)
    : _mem(mem),
      _minHops(minHops),
      _maxHops(maxHops) {
}

BFSExpandOutEdgesProcessor::~BFSExpandOutEdgesProcessor() {
}

std::string BFSExpandOutEdgesProcessor::describe() const {
    return fmt::format("BFSExpandOutEdgesProcessor @={} hops=[{},{}]",
                       fmt::ptr(this), _minHops, _maxHops);
}

BFSExpandOutEdgesProcessor* BFSExpandOutEdgesProcessor::create(
    PipelineV2* pipeline,
    LocalMemory* mem,
    int64_t minHops,
    int64_t maxHops) {
    auto* proc = new BFSExpandOutEdgesProcessor(mem, minHops, maxHops);

    PipelineInputPort* inPort = PipelineInputPort::create(pipeline, proc);
    PipelineOutputPort* outPort = PipelineOutputPort::create(pipeline, proc);

    proc->_input.setPort(inPort);
    proc->_output.setPort(outPort);

    proc->addInput(inPort);
    proc->addOutput(outPort);

    proc->postCreate(pipeline);

    return proc;
}

void BFSExpandOutEdgesProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    const GraphView& view = ctxt->getGraphView();

    // Input
    _inputSources = dynamic_cast<ColumnNodeIDs*>(_input.getNodeIDs()->getColumn());
    bioassert(_inputSources, "Input source nodes column is null");

    // Output
    bioassert(_outputTargets, "Output target nodes column is null");
    bioassert(_outputPaths, "Output paths column is null");
    bioassert(_outputIndices, "Output indices column is null");
    bioassert(dynamic_cast<ColumnVector<Path>*>(_outputPaths->getColumn()), "Output paths column is not a path column");
    bioassert(dynamic_cast<ColumnNodeIDs*>(_outputTargets->getColumn()), "Output targets column is not an entity ID column");

    // BFS Execution state
    _bfsSources = _mem->alloc<ColumnNodeIDs>();
    _bfsEdges = _mem->alloc<ColumnEdgeIDs>();
    _bfsIntermediates = _mem->alloc<ColumnNodeIDs>();
    _bfsIndices = _mem->alloc<ColumnIndices>();

    _bfsWriter = std::make_unique<GetOutEdgesChunkWriter>(view, _bfsSources);
    _bfsWriter->setIndices(_bfsIndices);
    _bfsWriter->setEdgeIDs(_bfsEdges);
    _bfsWriter->setTgtIDs(_bfsIntermediates);

    markAsPrepared();
}

void BFSExpandOutEdgesProcessor::reset() {
    markAsReset();
}

void BFSExpandOutEdgesProcessor::execute() {
    _outputIndices->clear();

    auto* outputTargets = static_cast<ColumnNodeIDs*>(_outputTargets->getColumn());
    auto* outputPaths = static_cast<ColumnVector<Path>*>(_outputPaths->getColumn());

    outputPaths->clear();

    struct FrontierEntry {
        NodeID node;
        Path edges;
        // bool _finished {false}; // TODO: Add this to the frontier
        //       and use it to avoid reexploring over and over again
    };

    const size_t inputSize = _inputSources->size();
    const ColumnNodeIDs& inputNodes = *_inputSources;
    std::vector<FrontierEntry> frontier;
    std::vector<FrontierEntry> nextFrontier;

    ColumnNodeIDs& bfsSources = *_bfsSources;
    ColumnEdgeIDs& bfsEdges = *_bfsEdges;
    ColumnNodeIDs& bfsIntermediates = *_bfsIntermediates;
    ColumnIndices& bfsIndices = *_bfsIndices;

    for (size_t sourceIdx = 0; sourceIdx < inputSize; sourceIdx++) {
        const NodeID sourceNode = inputNodes[sourceIdx];
        frontier.resize(1);
        frontier[0].node = sourceNode;
        frontier[0].edges = {};

        for (int64_t depth = 1; !frontier.empty(); depth++) {
            if (_maxHops != std::numeric_limits<int64_t>::max() && depth > _maxHops) {
                break;
            }

            // Prepare new set of sources
            bfsSources.resize(frontier.size());
            for (size_t i = 0; i < frontier.size(); i++) {
                bfsSources[i] = frontier[i].node;
            }

            bfsEdges.clear();
            bfsIntermediates.clear();
            bfsIndices.clear();

            _bfsWriter->reset();
            _bfsWriter->fill(SIZE_MAX); // TODO -> Make the bfs processor chunked

            nextFrontier.clear();
            for (size_t i = 0; i < bfsEdges.size(); i++) {
                const NodeID intermediate = bfsIntermediates[i];
                const size_t parentIdx = bfsIndices[i];
                const EntityID edgeVal = bfsEdges[i].getValue();

                const Path& parentPath = frontier[parentIdx].edges;
                const bool alreadyUsed = std::find(
                                             parentPath.begin(), parentPath.end(),
                                             edgeVal)
                                      != parentPath.end();
                if (alreadyUsed) {
                    continue;
                }

                FrontierEntry& newEntry = nextFrontier.emplace_back();
                newEntry.edges = parentPath; // Copy parent path
                newEntry.edges.push_back(edgeVal);
                newEntry.node = intermediate;

                if (depth >= _minHops) {
                    _outputIndices->push_back(sourceIdx);
                    outputPaths->push_back(newEntry.edges);
                }
            }

            frontier = std::move(nextFrontier);
        }
    }

    outputTargets->resize(frontier.size());
    for (size_t i = 0; i < frontier.size(); i++) {
        outputTargets->set(i, frontier[i].node);
    }

    _input.getPort()->consume();
    _output.getPort()->writeData();
    finish();
}
