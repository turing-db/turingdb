#pragma once

#include <memory>
#include <stdint.h>
#include <vector>

#include "Processor.h"

#include "interfaces/PipelineNodeInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"

#include "PathExplorationDir.h"
#include "EntityList.h"

namespace db {

class LocalMemory;
class Iterator;

class PathExplorerProcessor : public Processor {
public:
    static PathExplorerProcessor* create(PipelineV2* pipeline,
                                         LocalMemory* mem,
                                         PathExplorationDir dir,
                                         uint64_t minHops,
                                         uint64_t maxHops);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineNodeInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

    void setOutputPathsColumn(NamedColumn* paths) {
        _outputPaths = paths;
    }
    void setOutputTargetsColumn(NamedColumn* targetNodes) {
        _outputTargets = targetNodes;
    }
    void setOutputIndicesColumn(ColumnIndices* indices) {
        _outputIndices = indices;
    }

    NamedColumn* getOutputTargetsColumn() const {
        return _outputTargets;
    }
    NamedColumn* getOutputPathsColumn() const {
        return _outputPaths;
    }

private:
    struct FrontierEntry {
        NodeID node;
        EdgeID edge;                 // single edge that led to this node
        size_t parentIdx {SIZE_MAX}; // index into _allEntries, -1 for root
        size_t sourceIdx {SIZE_MAX}; // original input source index
    };

    PathExplorerProcessor(LocalMemory* mem,
                          PathExplorationDir dir,
                          uint64_t minHops,
                          uint64_t maxHops);
    ~PathExplorerProcessor() override;

    LocalMemory* _mem {nullptr};
    PathExplorationDir _dir {PathExplorationDir::Both};
    uint64_t _minHops {0};
    uint64_t _maxHops {0};

    PipelineNodeInputInterface _input;
    PipelineBlockOutputInterface _output;

    // Processor input/outputs
    ColumnNodeIDs* _inputSources {nullptr};
    NamedColumn* _outputTargets {nullptr};
    NamedColumn* _outputPaths {nullptr};
    ColumnIndices* _outputIndices {nullptr};

    // BFS chunk writer internals
    ColumnNodeIDs* _bfsSources {nullptr};
    ColumnEdgeIDs* _bfsEdges {nullptr};
    ColumnNodeIDs* _bfsIntermediates {nullptr};
    ColumnIndices* _bfsIndices {nullptr};
    std::unique_ptr<Iterator> _bfsWriter;

    // Persistent state across execute() calls
    bool _bfsInitialized {false};
    bool _depthNeedsSetup {true};
    uint64_t _depth {0};
    std::vector<FrontierEntry> _allEntries; // persistent tree, never shrunk until new input chunk received
    size_t _depthStart {0};                 // index of first entry at current depth
    size_t _depthEnd {0};                   // index one past last entry at current depth

    bool edgeUsedInPath(size_t entryIdx, EdgeID edge) const;
    void reconstructPath(size_t entryIdx, EntityList& path) const;
};

}
