#include "CommitBuilder.h"

#include "reader/GraphReader.h"
#include "Graph.h"
#include "spdlog/spdlog.h"
#include "versioning/Commit.h"
#include "writers/DataPartBuilder.h"

using namespace db;

CommitBuilder::CommitBuilder() = default;

CommitBuilder::~CommitBuilder() = default;

std::unique_ptr<CommitBuilder> CommitBuilder::prepare(Graph& graph, const GraphView& view) {
    const auto reader = view.read();

    auto* ptr = new CommitBuilder {graph, view};

    ptr->_firstNodeID = reader.getNodeCount();
    ptr->_firstEdgeID = reader.getNodeCount();
    ptr->_previousDataparts = reader.dataparts();

    return std::unique_ptr<CommitBuilder> {ptr};
}


DataPartBuilder& CommitBuilder::newBuilder() {
    std::scoped_lock lock {_mutex};
    auto& builder = _builders.emplace_back(DataPartBuilder::prepare(*_graph,
                                                                    _view,
                                                                    _nextNodeID,
                                                                    _nextEdgeID));

    return *builder;
}

DataPartBuilder& CommitBuilder::newBuilder(size_t nodeCount, size_t edgeCount) {
    std::scoped_lock lock {_mutex};
    auto& builder = _builders.emplace_back(DataPartBuilder::prepare(*_graph,
                                                                    _view,
                                                                    _nextNodeID,
                                                                    _nextEdgeID));
    _nextNodeID += nodeCount;
    _nextEdgeID += edgeCount;

    return *builder;
}

std::unique_ptr<Commit> CommitBuilder::build(Graph& graph, JobSystem& jobsystem) {
    spdlog::info("** Building commit");
    std::scoped_lock lock {_mutex};
    size_t nodeCount = 0;
    size_t edgeCount = 0;

    spdlog::info("- Builders:");
    for (const auto& builder : _builders) {
        spdlog::info("    * {} nodes, {} edges", builder->nodeCount(), builder->edgeCount());
        nodeCount += builder->nodeCount();
        edgeCount += builder->edgeCount();
    }

    auto [firstNodeID, firstEdgeID] = graph.allocIDRange(nodeCount, edgeCount);

    auto commit = std::make_unique<Commit>();
    commit->_graph = &graph;
    commit->_data = std::make_shared<CommitData>();
    commit->_data->_hash = commit->_hash;
    commit->_data->_graphMetadata = graph.getMetadata();
    commit->_data->_dataparts = _view.data()._dataparts;

    size_t i = 0;
    for (const auto& builder : _builders) {
        spdlog::info("- Building datapart of builder {}", i + 1);
        auto part = std::make_shared<DataPart>(firstNodeID, firstEdgeID);

        firstNodeID += builder->nodeCount();
        firstEdgeID += builder->nodeCount();

        part->load(_view, jobsystem, *builder);
        commit->_data->_dataparts.emplace_back(part);
        i++;
    }
    spdlog::info("- Done building commit");

    return commit;
}

CommitBuilder::CommitBuilder(Graph& graph, const GraphView& view)
    : _graph(&graph),
      _versionController(graph._versionController.get()),
      _view(view) {
}

