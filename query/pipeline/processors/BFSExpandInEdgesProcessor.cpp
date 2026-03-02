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
    _maxHops(maxHops)
{
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
    _inputTargets = dynamic_cast<ColumnNodeIDs*>(
        _input.getNodeIDs()->getColumn());
    bioassert(_inputTargets, "Input target nodes column is null");

    // Output
    bioassert(_outputSources, "Output sources nodes column is null");
    bioassert(_outputPaths, "Output paths column is null");
    bioassert(_outputIndices, "Output indices column is null");
    bioassert(
        dynamic_cast<ColumnVector<EntityList>*>(
            _outputPaths->getColumn()),
        "Output paths column is not a path column");
    bioassert(
        dynamic_cast<ColumnNodeIDs*>(_outputSources->getColumn()),
        "Output sources column is not an entity ID column");

    // BFS Execution state
    _bfsTargets = _mem->alloc<ColumnNodeIDs>();
    _bfsEdges = _mem->alloc<ColumnEdgeIDs>();
    _bfsIntermediates = _mem->alloc<ColumnNodeIDs>();
    _bfsIndices = _mem->alloc<ColumnIndices>();

    _bfsWriter = std::make_unique<GetInEdgesChunkWriter>(
        view, _bfsTargets);
    _bfsWriter->setIndices(_bfsIndices);
    _bfsWriter->setEdgeIDs(_bfsEdges);
    _bfsWriter->setSrcIDs(_bfsIntermediates);

    markAsPrepared();
}

void BFSExpandInEdgesProcessor::reset() {
    _bfsInitialized = false;
    _depthNeedsSetup = true;
    _depth = 0;
    _frontier.clear();
    _nextFrontier.clear();
    _mainMapping.clear();
    _nextMainMapping.clear();
    markAsReset();
}

void BFSExpandInEdgesProcessor::execute() {
    auto* outputSources =
        _outputSources->getColumn()->cast<ColumnNodeIDs>();
    auto* outputPaths =
        _outputPaths->getColumn()->cast<ColumnVector<EntityList>>();

    // Clear output columns for this chunk
    outputSources->clear();
    outputPaths->clear();
    _outputIndices->clear();

    // Initialize BFS on first call
    if (!_bfsInitialized) {
        _bfsInitialized = true;

        const ColumnNodeIDs& inputNodes = *_inputTargets;
        const size_t inputSize = inputNodes.size();

        _frontier.resize(inputSize);
        _mainMapping.resize(inputSize);

        for (size_t i = 0; i < inputSize; i++) {
            _frontier[i].node = inputNodes[i];
            _mainMapping[i] = i;
        }

        _depth = 1;
        _depthNeedsSetup = true;
    }

    // Set up current depth if needed
    if (_depthNeedsSetup) {
        if (_frontier.empty() || _depth > _maxHops) {
            // Reached max depth or no more nodes to expand
            _input.getPort()->consume();
            _output.getPort()->writeData();
            finish();
            return;
        }

        _bfsTargets->resize(_frontier.size());
        for (size_t i = 0; i < _frontier.size(); i++) {
            (*_bfsTargets)[i] = _frontier[i].node;
        }

        _bfsWriter->reset();
        _nextFrontier.clear();
        _nextMainMapping.clear();
        _depthNeedsSetup = false;
    }

    // Expand one chunk at current depth
    _bfsWriter->fill(_ctxt->getChunkSize());

    // Process edges from this fill
    ColumnEdgeIDs& bfsEdges = *_bfsEdges;
    ColumnNodeIDs& bfsIntermediates = *_bfsIntermediates;
    ColumnIndices& bfsIndices = *_bfsIndices;

    EntityList::Entry edgeVal {
        ._type = EntityType::Edge,
    };

    for (size_t i = 0; i < bfsEdges.size(); i++) {
        const NodeID intermediate = bfsIntermediates[i];
        const size_t parentIdx = bfsIndices[i];
        edgeVal._id = bfsEdges[i].getValue();

        const EntityList& parentPath =
            _frontier[parentIdx].edges;
        const bool alreadyUsed = std::find(
                                     parentPath.begin(),
                                     parentPath.end(),
                                     edgeVal)
                              != parentPath.end();
        if (alreadyUsed) {
            // Already visited this edge, skip it
            continue;
        }

        FrontierEntry& newEntry = _nextFrontier.emplace_back();
        newEntry.edges = parentPath;
        newEntry.edges.add(edgeVal._type, edgeVal._id);
        newEntry.node = intermediate;
        _nextMainMapping.push_back(_mainMapping[parentIdx]);

        if (_depth >= _minHops) {
            _outputIndices->push_back(_mainMapping[parentIdx]);
            outputSources->push_back(intermediate);
            outputPaths->push_back(newEntry.edges);
        }
    }

    // Check if current depth is fully expanded
    if (!_bfsWriter->isValid()) {
        _frontier = std::move(_nextFrontier);
        _mainMapping = std::move(_nextMainMapping);
        _depth++;
        _depthNeedsSetup = true;

        // Check if BFS is complete
        if (_frontier.empty() || _depth > _maxHops) {
            _input.getPort()->consume();
            finish();
        }
    }

    _output.getPort()->writeData();
}
