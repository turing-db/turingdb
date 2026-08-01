#pragma once

#include <limits>
#include <memory>
#include <optional>
#include <stdint.h>
#include <type_traits>
#include <vector>

#include "Processor.h"

#include "interfaces/PipelineNodeInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"

#include "PathExplorationDir.h"
#include "EntityList.h"
#include "ID.h"

namespace db {

class Iterator;
class GetEdgesChunkWriter;
class GetOutEdgesChunkWriter;
class GetInEdgesChunkWriter;

template <PathExplorationDir Dir>
class PathExplorerProcessor : public Processor {
public:
    static PathExplorerProcessor* create(PipelineV2* pipeline,
                                         uint64_t minHops,
                                         uint64_t maxHops);

    using BFSChunkWriter = std::conditional_t<
        Dir == PathExplorationDir::BOTH,
        GetEdgesChunkWriter,
        std::conditional_t<Dir == PathExplorationDir::FORWARD,
                           GetOutEdgesChunkWriter,
                           GetInEdgesChunkWriter>>;

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
    void setBfsIndicesColumn(ColumnIndices* indices) {
        _bfsIndices = indices;
    }
    void setBfsEdgesColumn(ColumnEdgeIDs* edges) {
        _bfsEdges = edges;
    }
    void setBfsIntermediatesColumn(ColumnNodeIDs* intermediates) {
        _bfsIntermediates = intermediates;
    }
    void setBfsSourcesColumn(ColumnNodeIDs* sources) {
        _bfsSources = sources;
    }
    void setBfsEdgeTypesColumn(ColumnEdgeTypes* types) {
        _bfsEdgeTypes = types;
    }
    void setEdgeTypeConstraint(const std::optional<EdgeTypeID>& edgeType) {
        _edgeTypeConstraint = edgeType;
    }

    NamedColumn* getOutputTargetsColumn() const {
        return _outputTargets;
    }
    NamedColumn* getOutputPathsColumn() const {
        return _outputPaths;
    }

private:
    static constexpr size_t ROOT = std::numeric_limits<size_t>::max();

    struct FrontierEntry {
        NodeID node;
        EdgeID edge;
        size_t parentIdx {ROOT};
        size_t sourceIdx {ROOT};
    };

    PathExplorerProcessor(uint64_t minHops,
                          uint64_t maxHops);
    ~PathExplorerProcessor() override;

    uint64_t _minHops {0};
    uint64_t _maxHops {0};

    PipelineNodeInputInterface _input;
    PipelineBlockOutputInterface _output;

    ColumnNodeIDs* _inputSources {nullptr};
    NamedColumn* _outputTargets {nullptr};
    NamedColumn* _outputPaths {nullptr};
    ColumnIndices* _outputIndices {nullptr};

    ColumnNodeIDs* _bfsSources {nullptr};
    ColumnEdgeIDs* _bfsEdges {nullptr};
    ColumnNodeIDs* _bfsIntermediates {nullptr};
    ColumnIndices* _bfsIndices {nullptr};
    ColumnEdgeTypes* _bfsEdgeTypes {nullptr};
    std::unique_ptr<BFSChunkWriter> _bfsWriter;
    std::optional<EdgeTypeID> _edgeTypeConstraint {};

    bool _bfsInitialized {false};
    bool _depthNeedsSetup {true};

    uint64_t _depth {0};

    std::vector<FrontierEntry> _allEntries;

    size_t _depthStart {0};

    size_t _depthEnd {0};

    size_t _candidateEdges {0};
    size_t _emittedRows {0};

    bool edgeUsedInPath(size_t entryIdx, EdgeID edge) const;
    void reconstructPath(size_t entryIdx, EntityList& path) const;
};

}
