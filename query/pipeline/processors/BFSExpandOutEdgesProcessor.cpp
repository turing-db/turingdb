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

#include "EntityList.h"
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
    bioassert(_inputSources, "Input nodes column is null");

    // Output
    bioassert(_outputTargets, "Output target nodes column is null");
    bioassert(_outputPaths, "Output paths column is null");
    bioassert(_outputIndices, "Output indices column is null");
    bioassert(dynamic_cast<ColumnVector<EntityList>*>(_outputPaths->getColumn()), "Output paths column is not a path column");
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
    auto* outputTargets = static_cast<ColumnNodeIDs*>(_outputTargets->getColumn());
    auto* outputPaths = static_cast<ColumnVector<EntityList>*>(_outputPaths->getColumn());

    outputPaths->clear();

    struct FrontierEntry {
        NodeID node;
        EntityList edges;
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

    // 1. InputNodes = whatever we received
    // 2. Expand once, we get edges + indices  
    // 3. We fill a frontier vector (some slots might be empty, then will have to be filtered out)>
    //    At the same time, we fill the next input vector (the intermediates)
    // 4. We repeat 2. and 3. until we reach the maxHops (or no more results)
    //    The indices reference the intermediate vector, we need to map it back to the frontier vector
    //
    //    Say we had 10000 inputs, and got only 2 results after the first step, we may have indices = [ 500, 501 ]
    //    We expand again, and get 5 edges with indices  = [ 0, 0, 0, 1, 1 ]
    //    We need to know that 0 -> 500 and 1 -> 501
    //    Next expand, say we get a single edge with index = 4
    //    We need to know that 0 --> 4 --> 1 --> 501
    //
    //    This can be done by storing intermediate indices: [ chunk writer indices ] -> [ main mapping ]
    //    - [ 500, 501 ] -> OK
    //    - [ 0, 0, 0, 1, 1 ] -> [ 500, 500, 500, 501, 501 ]
    //    - [ 4 ] -> [ 501 ]
    //    
    //  So the actual algorithm is:
    //  0. - mainMapping: ColumnIndices (size 0 for now)
    //     - frontier = vector<Entry>(inputSize)
    //
    //  1. - InputNodes = whatever we received, put all of them in a single vector and give them to the GetOutEdgesChunkWriter
    //     - Also give it the bfsIndices vector
    //     - Run fill()
    //     - Create the mapping from bfsIndices to the frontier by filling mainMapping with the indices
    //  2. - Repeat, but inputNodes = bfsIntermediates
    //     - Run fill()
    //     - Using the new bfsIndices: update mainMapping (they should always point  the slots in the inputNodes vector) 
    //     - Repeat until we reach the maxHops or no more results

    // 0. Initialize frontier with all input nodes.
    //    mainMapping[i] = original input index for frontier entry i.
    frontier.resize(inputSize);
    std::vector<size_t> mainMapping(inputSize);
    for (size_t i = 0; i < inputSize; i++) {
        frontier[i].node = inputNodes[i];
        mainMapping[i] = i;
    }

    std::vector<size_t> nextMainMapping;

    for (int64_t depth = 1; !frontier.empty(); depth++) {
        if (depth > _maxHops) {
            break;
        }

        // 1. Feed all frontier nodes to the chunk writer
        bfsSources.resize(frontier.size());
        for (size_t i = 0; i < frontier.size(); i++) {
            bfsSources[i] = frontier[i].node;
        }

        bfsEdges.clear();
        bfsIntermediates.clear();
        bfsIndices.clear();

        _bfsWriter->reset();
        _bfsWriter->fill(SIZE_MAX);

        if (bfsEdges.empty()) {
            break;
        }

        // 2. Build next frontier from expanded edges.
        //    bfsIndices[j] references the frontier entry that produced
        //    edge j. We compose with mainMapping to get the original
        //    input index.
        EntityList::Entry edgeVal {
            ._type = EntityType::Edge,
        };

        nextFrontier.clear();
        nextMainMapping.clear();

        for (size_t i = 0; i < bfsEdges.size(); i++) {
            const NodeID intermediate = bfsIntermediates[i];
            const size_t parentIdx = bfsIndices[i];
            edgeVal._id = bfsEdges[i].getValue();

            const EntityList& parentPath = frontier[parentIdx].edges;
            const bool alreadyUsed = std::find(
                                         parentPath.begin(),
                                         parentPath.end(),
                                         edgeVal)
                                  != parentPath.end();
            if (alreadyUsed) {
                continue;
            }

            FrontierEntry& newEntry = nextFrontier.emplace_back();
            newEntry.edges = parentPath;
            newEntry.edges.add(edgeVal._type, edgeVal._id);
            newEntry.node = intermediate;
            nextMainMapping.push_back(mainMapping[parentIdx]);

            if (depth >= _minHops) {
                _outputIndices->push_back(mainMapping[parentIdx]);
                outputTargets->push_back(intermediate);
                outputPaths->push_back(newEntry.edges);
            }
        }

        frontier = std::move(nextFrontier);
        mainMapping = std::move(nextMainMapping);
    }
    fmt::println("BFSExpandOutEdgesProcessor::execute(): done. {} edges", outputPaths->size());

    _input.getPort()->consume();
    _output.getPort()->writeData();
    finish();
}
