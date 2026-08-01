#include "PathExplorerProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"
#include "ExecutionContext.h"

#include "dataframe/NamedColumn.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnIndices.h"
#include "columns/ColumnEdgeTypes.h"
#include "iterators/GetOutEdgesIterator.h"
#include "iterators/GetInEdgesIterator.h"
#include "iterators/GetEdgesIterator.h"
#include "EntityList.h"
#include "PipelineBuffer.h"

#include "Panic.h"

using namespace db;

template <PathExplorationDir Dir>
PathExplorerProcessor<Dir>::PathExplorerProcessor(uint64_t minHops,
                                                  uint64_t maxHops)
    : _minHops(minHops),
    _maxHops(maxHops)
{
}

template <PathExplorationDir Dir>
PathExplorerProcessor<Dir>::~PathExplorerProcessor() {
}

template <PathExplorationDir Dir>
std::string PathExplorerProcessor<Dir>::describe() const {
    return fmt::format("PathExplorerProcessor @={} hops=[{},{}] candidates={} emitted={}",
                       fmt::ptr(this),
                       _minHops,
                       _maxHops,
                       _candidateEdges,
                       _emittedRows);
}

template <PathExplorationDir Dir>
PathExplorerProcessor<Dir>* PathExplorerProcessor<Dir>::create(PipelineV2* pipeline,
                                                               uint64_t minHops,
                                                               uint64_t maxHops) {
    auto* proc = new PathExplorerProcessor<Dir>(minHops, maxHops);

    PipelineInputPort* inPort = PipelineInputPort::create(pipeline, proc);
    PipelineOutputPort* outPort = PipelineOutputPort::create(pipeline, proc);

    proc->_input.setPort(inPort);
    proc->_output.setPort(outPort);

    proc->addInput(inPort);
    proc->addOutput(outPort);

    proc->postCreate(pipeline);

    return proc;
}

template <PathExplorationDir Dir>
void PathExplorerProcessor<Dir>::prepare(ExecutionContext* ctxt) {
    _ctxt = ctxt;
    const GraphView& view = ctxt->getGraphView();

    _inputSources = _input.getNodeIDs()->as<ColumnNodeIDs>();
    bioassert(_inputSources, "Input nodes column is null");

    bioassert(_outputTargets, "Output target nodes column is null");
    bioassert(_outputPaths, "Output paths column is null");
    bioassert(_outputIndices, "Output indices column is null");
    bioassert(_outputPaths->as<ColumnVector<EntityList>>(),
              "Output paths column is not an entity list column");
    bioassert(_outputTargets->as<ColumnNodeIDs>(),
              "Output targets column is not a node ID column");

    if constexpr (Dir == PathExplorationDir::BOTH) {
        _bfsWriter = std::make_unique<BFSChunkWriter>(view, _bfsSources);
        _bfsWriter->setIndices(_bfsIndices);
        _bfsWriter->setEdgeIDs(_bfsEdges);
        _bfsWriter->setOtherIDs(_bfsIntermediates);
        if (_edgeTypeConstraint) {
            _bfsWriter->setEdgeTypes(_bfsEdgeTypes);
        }
    } else if constexpr (Dir == PathExplorationDir::FORWARD) {
        _bfsWriter = std::make_unique<BFSChunkWriter>(view, _bfsSources);
        _bfsWriter->setIndices(_bfsIndices);
        _bfsWriter->setEdgeIDs(_bfsEdges);
        _bfsWriter->setTgtIDs(_bfsIntermediates);
        if (_edgeTypeConstraint) {
            _bfsWriter->setEdgeTypes(_bfsEdgeTypes);
        }
    } else if constexpr (Dir == PathExplorationDir::BACKWARD) {
        _bfsWriter = std::make_unique<BFSChunkWriter>(view, _bfsSources);
        _bfsWriter->setIndices(_bfsIndices);
        _bfsWriter->setEdgeIDs(_bfsEdges);
        _bfsWriter->setSrcIDs(_bfsIntermediates);
        if (_edgeTypeConstraint) {
            _bfsWriter->setEdgeTypes(_bfsEdgeTypes);
        }
    } else {
        COMPILE_ERROR("Invalid PathExplorationDir");
    }

    markAsPrepared();
}

template <PathExplorationDir Dir>
void PathExplorerProcessor<Dir>::reset() {
    _bfsInitialized = false;
    _depthNeedsSetup = true;
    _depth = 0;
    _allEntries.clear();
    _depthStart = 0;
    _depthEnd = 0;
    _candidateEdges = 0;
    _emittedRows = 0;
    markAsReset();
}

template <PathExplorationDir Dir>
void PathExplorerProcessor<Dir>::reconstructPath(size_t entryIdx, EntityList& path) const {
    path.resize(_depth);

    size_t idx = entryIdx;

    size_t pathCursor = _depth - 1;

    while (idx != ROOT) {
        const FrontierEntry& e = _allEntries[idx];

        if (e.parentIdx == ROOT) {
            break;
        }

        auto& out = path[pathCursor--];
        out._type = EntityType::Edge;
        out._id = e.edge.getValue();

        idx = e.parentIdx;
    }
}

template <PathExplorationDir Dir>
bool PathExplorerProcessor<Dir>::edgeUsedInPath(size_t entryIdx, EdgeID edge) const {
    size_t idx = entryIdx;

    while (idx != ROOT) {
        const FrontierEntry& e = _allEntries[idx];

        if (e.parentIdx != ROOT && e.edge == edge) {
            return true;
        }

        idx = e.parentIdx;
    }

    return false;
}

template <PathExplorationDir Dir>
void PathExplorerProcessor<Dir>::execute() {
    auto* outputTargets = _outputTargets->as<ColumnNodeIDs>();
    auto* outputPaths = _outputPaths->as<ColumnVector<EntityList>>();

    outputTargets->clear();
    outputPaths->clear();
    _outputIndices->clear();

    size_t remaining = _ctxt->getChunkSize();

    if (!_bfsInitialized) {
        _bfsInitialized = true;

        const ColumnNodeIDs& inputNodes = *_inputSources;
        const size_t inputSize = inputNodes.size();

        _allEntries.resize(inputSize);
        for (size_t i = 0; i < inputSize; i++) {
            _allEntries[i] = FrontierEntry {
                .node = inputNodes[i],
                .edge = EdgeID {},
                .parentIdx = ROOT,
                .sourceIdx = i,
            };
        }

        if (_minHops == 0) {
            for (size_t i = 0; i < inputSize; i++) {
                _outputIndices->push_back(i);
                outputTargets->push_back(inputNodes[i]);
                outputPaths->emplace_back();
            }

            _emittedRows += inputSize;

            remaining = remaining >= inputSize
                          ? remaining - inputSize
                          : 0;
        }

        _depthStart = 0;
        _depthEnd = inputSize;
        _depth = 1;
        _depthNeedsSetup = true;
    }

    bioassert(_depth > 0,
              "Initialization error, depth should always be greater than 0 at this point");

    if (_depthNeedsSetup) {
        if (_depth > _maxHops || _depthStart == _depthEnd) {
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

        _bfsWriter->reset();

        _depthNeedsSetup = false;
    }

    _bfsWriter->fill(remaining);

    ColumnEdgeIDs& bfsEdges = *_bfsEdges;
    ColumnNodeIDs& bfsIntermediates = *_bfsIntermediates;
    ColumnIndices& bfsIndices = *_bfsIndices;

    _candidateEdges += bfsEdges.size();

    for (size_t i = 0; i < bfsEdges.size(); i++) {
        const NodeID intermediate = bfsIntermediates[i];
        const EdgeID edge = bfsEdges[i];

        if (_edgeTypeConstraint) {
            if ((*_bfsEdgeTypes)[i] != *_edgeTypeConstraint) {
                continue;
            }
        }

        const size_t parentIdx = _depthStart + bfsIndices[i];

        if (edgeUsedInPath(parentIdx, edge)) {
            continue;
        }

        const size_t newIdx = _allEntries.size();
        _allEntries.push_back(FrontierEntry {
            .node = intermediate,
            .edge = edge,
            .parentIdx = parentIdx,
            .sourceIdx = _allEntries[parentIdx].sourceIdx,
        });

        if (_depth >= _minHops) {
            _outputIndices->push_back(_allEntries[parentIdx].sourceIdx);
            outputTargets->push_back(intermediate);
            EntityList& p = outputPaths->emplace_back();
            reconstructPath(newIdx, p);
            _emittedRows++;
        }
    }

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

namespace db {

template class PathExplorerProcessor<PathExplorationDir::BOTH>;
template class PathExplorerProcessor<PathExplorationDir::FORWARD>;
template class PathExplorerProcessor<PathExplorationDir::BACKWARD>;

}
