#include "PathExplorerProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"
#include "LocalMemory.h"

#include "dataframe/NamedColumn.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnIndices.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetEdgesIterator.h"

#include "EntityList.h"
#include "PipelineBuffer.h"

using namespace db;

PathExplorerProcessor::PathExplorerProcessor(LocalMemory* mem,
                                             PathExplorationDir dir,
                                             int64_t minHops,
                                             int64_t maxHops)
    : _mem(mem),
    _dir(dir),
    _minHops(minHops),
    _maxHops(maxHops)
{
}

PathExplorerProcessor::~PathExplorerProcessor() {
}

std::string PathExplorerProcessor::describe() const {
    return fmt::format("PathExplorerProcessor @={} hops=[{},{}]",
                       fmt::ptr(this), _minHops, _maxHops);
}

PathExplorerProcessor* PathExplorerProcessor::create(PipelineV2* pipeline,
                                                     LocalMemory* mem,
                                                     PathExplorationDir dir,
                                                     int64_t minHops,
                                                     int64_t maxHops) {
    auto* proc = new PathExplorerProcessor(mem, dir, minHops, maxHops);

    PipelineInputPort* inPort = PipelineInputPort::create(pipeline, proc);
    PipelineOutputPort* outPort = PipelineOutputPort::create(pipeline, proc);

    proc->_input.setPort(inPort);
    proc->_output.setPort(outPort);

    proc->addInput(inPort);
    proc->addOutput(outPort);

    proc->postCreate(pipeline);

    return proc;
}

void PathExplorerProcessor::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    const GraphView& view = ctxt->getGraphView();

    // Input
    _inputSources = dynamic_cast<ColumnNodeIDs*>(_input.getNodeIDs()->getColumn());
    bioassert(_inputSources, "Input nodes column is null");

    // Output
    bioassert(_outputTargets, "Output target nodes column is null");
    bioassert(_outputPaths, "Output paths column is null");
    bioassert(_outputIndices, "Output indices column is null");
    bioassert(dynamic_cast<ColumnVector<EntityList>*>(_outputPaths->getColumn()),
              "Output paths column is not a path column");
    bioassert(dynamic_cast<ColumnNodeIDs*>(_outputTargets->getColumn()),
              "Output targets column is not an entity ID column");

    // BFS Execution state
    _bfsSources = _mem->alloc<ColumnNodeIDs>();
    _bfsEdges = _mem->alloc<ColumnEdgeIDs>();
    _bfsIntermediates = _mem->alloc<ColumnNodeIDs>();
    _bfsIndices = _mem->alloc<ColumnIndices>();

    switch (_dir) {
        case PathExplorationDir::Forward: {
            auto* bfsWriter = new GetOutEdgesChunkWriter(view, _bfsSources);
            bfsWriter->setIndices(_bfsIndices);
            bfsWriter->setEdgeIDs(_bfsEdges);
            bfsWriter->setTgtIDs(_bfsIntermediates);
            _bfsWriter = std::unique_ptr<Iterator>(bfsWriter);
        } break;
        case PathExplorationDir::Backward: {
            auto* bfsWriter = new GetInEdgesChunkWriter(view, _bfsSources);
            bfsWriter->setIndices(_bfsIndices);
            bfsWriter->setEdgeIDs(_bfsEdges);
            bfsWriter->setSrcIDs(_bfsIntermediates);
            _bfsWriter = std::unique_ptr<Iterator>(bfsWriter);
        } break;
        case PathExplorationDir::Both: {
            auto* bfsWriter = new GetEdgesChunkWriter(view, _bfsSources);
            bfsWriter->setIndices(_bfsIndices);
            bfsWriter->setEdgeIDs(_bfsEdges);
            bfsWriter->setOtherIDs(_bfsIntermediates);
            _bfsWriter = std::unique_ptr<Iterator>(bfsWriter);
        } break;
    }
    markAsPrepared();
}

void PathExplorerProcessor::reset() {
    _bfsInitialized = false;
    _depthNeedsSetup = true;
    _depth = 0;
    _allEntries.clear();
    _depthStart = 0;
    _depthEnd = 0;
    markAsReset();
}

void PathExplorerProcessor::reconstructPath(size_t entryIdx, EntityList& path) const {
    size_t idx = entryIdx;

    path.resize(_depth);

    size_t i = _depth - 1;

    // Fill the path in reverse order
    while (idx != SIZE_MAX) {
        const FrontierEntry& e = _allEntries[idx];

        if (e.parentIdx != SIZE_MAX) {
            auto& entry = path[i--];
            entry._type = EntityType::Edge;
            entry._id = e.edge.getValue();
        }

        idx = e.parentIdx;
    }
}

/// Walk up the parent chain to check if edge is already used in the current row.
bool PathExplorerProcessor::edgeUsedInPath(size_t entryIdx, EdgeID edge) const {
    size_t idx = entryIdx;

    while (idx != SIZE_MAX) {
        const FrontierEntry& e = _allEntries[idx];

        if (e.parentIdx != SIZE_MAX && e.edge == edge) {
            return true;
        }

        idx = e.parentIdx;
    }

    return false;
}

void PathExplorerProcessor::execute() {
    auto* outputTargets = _outputTargets->getColumn()->cast<ColumnNodeIDs>();
    auto* outputPaths = _outputPaths->getColumn()->cast<ColumnVector<EntityList>>();

    outputTargets->clear();
    outputPaths->clear();
    _outputIndices->clear();

    size_t remaining = _ctxt->getChunkSize();

    if (!_bfsInitialized) {
        // Step 1. This is the first time execute() is called on the input chunk.
        //         It initializes the breadth-first exploration on it.
        _bfsInitialized = true;

        const ColumnNodeIDs& inputNodes = *_inputSources;
        const size_t inputSize = inputNodes.size();

        // Seed _allEntries with root entries (one per input source node).
        // Root entries have parentIdx = -1 and no edge.
        _allEntries.resize(inputSize);
        for (size_t i = 0; i < inputSize; i++) {
            _allEntries[i] = FrontierEntry {
                .node = inputNodes[i],
                .edge = EdgeID {},
                .parentIdx = SIZE_MAX,
                .sourceIdx = i,
            };
        }

        if (_minHops == 0) {
            // Write all paths corresponding to no hops
            for (size_t i = 0; i < inputSize; i++) {
                _outputIndices->push_back(i);
                outputTargets->push_back(inputNodes[i]);
                outputPaths->emplace_back();
            }

            remaining = remaining >= inputSize
                          ? remaining - inputSize
                          : 0;
        }

        _depthStart = 0;
        _depthEnd = inputSize;
        _depth = 1;
        _depthNeedsSetup = true;
    }

    if (_depthNeedsSetup) {
        // Step 2. This is the first time execute() is called on the current depth.
        //         It sets up the current depth window and feeds it to the BFS writer.
        //         It prepares a chunk of size `windowSize` to be fed to the ChunkWriter.
        //
        //         > [!WARNING]
        //         > `windowSize` can exceed the maximum ChunkSize
        //         >   -> the memory usage is unbounded.

        if (_depthStart == _depthEnd || _depth > _maxHops) {
            _input.getPort()->consume();
            _output.getPort()->writeData();
            finish();
            return;
        }

        const size_t windowSize = _depthEnd - _depthStart;

        _bfsSources->resize(windowSize);
        for (size_t i = 0; i < windowSize; i++) {
            (*_bfsSources)[i] = _allEntries[_depthStart + i].node;
        }

        switch (_dir) {
            case PathExplorationDir::Forward: {
                static_cast<GetOutEdgesChunkWriter*>(_bfsWriter.get())->reset();
            } break;
            case PathExplorationDir::Backward: {
                static_cast<GetInEdgesChunkWriter*>(_bfsWriter.get())->reset();
            } break;
            case PathExplorationDir::Both: {
                static_cast<GetEdgesChunkWriter*>(_bfsWriter.get())->reset();
            } break;
        }

        _depthNeedsSetup = false;
    }

    // Step 3. Fill one chunk of edges from the current depth set of sources.
    switch (_dir) {
        case PathExplorationDir::Forward: {
            static_cast<GetOutEdgesChunkWriter*>(_bfsWriter.get())->fill(remaining);
        } break;
        case PathExplorationDir::Backward: {
            static_cast<GetInEdgesChunkWriter*>(_bfsWriter.get())->fill(remaining);
        } break;
        case PathExplorationDir::Both: {
            static_cast<GetEdgesChunkWriter*>(_bfsWriter.get())->fill(remaining);
        } break;
    }

    ColumnEdgeIDs& bfsEdges = *_bfsEdges;
    ColumnNodeIDs& bfsIntermediates = *_bfsIntermediates;
    ColumnIndices& bfsIndices = *_bfsIndices;

    for (size_t i = 0; i < bfsEdges.size(); i++) {
        // Step 4. For each edges at this depth:
        //           - Check if the edge is already used in the current path.
        //           - If not, add it to the entries (to be used by deeper levels).
        //           - Reconstruct and write the path in the output if minHops is met.

        const NodeID intermediate = bfsIntermediates[i];
        const EdgeID edge = bfsEdges[i];

        // `parentIdx` references the parent of the current edge in the
        // frontier entries array
        const size_t parentIdx = _depthStart + bfsIndices[i];

        // Per-path edge uniqueness check — O(depth) walk up parent chain
        // To check if the edge was already encountered in the current path
        if (edgeUsedInPath(parentIdx, edge)) {
            continue;
        }

        // Append new entry to the persistent frontier entries store
        const size_t newIdx = _allEntries.size();
        _allEntries.push_back(FrontierEntry {
            .node = intermediate,
            .edge = edge,
            .parentIdx = parentIdx,
            .sourceIdx = _allEntries[parentIdx].sourceIdx,
        });

        // If the current depth is at least minHops, write the path to the output
        // The path is reconstructed by walking up the parent chain
        if (_depth >= _minHops) {
            _outputIndices->push_back(_allEntries[parentIdx].sourceIdx);
            outputTargets->push_back(intermediate);
            EntityList& p = outputPaths->emplace_back();
            reconstructPath(newIdx, p);
        }
    }

    // Current depth fully expanded — advance to next depth. Next execute()
    // call will trigger Step 2), as well as Step 1 if the input chunk is exhausted)
    if (!_bfsWriter->isValid()) {
        _depthStart = _depthEnd;
        _depthEnd = _allEntries.size();
        _depth++;
        _depthNeedsSetup = true;

        if (_depthStart == _depthEnd || _depth > _maxHops) {
            _input.getPort()->consume();
            finish();
        }
    }

    _output.getPort()->writeData();
}
