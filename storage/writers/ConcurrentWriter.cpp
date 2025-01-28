#include "ConcurrentWriter.h"

#include <mutex>
#include <range/v3/view/enumerate.hpp>

#include "Graph.h"
#include "DataPartManager.h"
#include "views/GraphView.h"
#include "JobSystem.h"
#include "JobGroup.h"

namespace db {

ConcurrentWriter::ConcurrentWriter(EntityID firstNodeID,
                                   EntityID firstEdgeID,
                                   Graph* graph)
    : _firstNodeID(firstNodeID),
      _firstEdgeID(firstEdgeID),
      _graph(graph)
{
}

ConcurrentWriter::~ConcurrentWriter() = default;

DataPartBuilder& ConcurrentWriter::newBuilder(size_t nodeCount, size_t edgeCount) {
    std::unique_lock lock(_mutex);
    auto builder = std::make_unique<DataPartBuilder>(_nextNodeID, _nextEdgeID, _graph);
    DataPartBuilder* ptr = builder.get();
    _nextNodeID += nodeCount;
    _nextEdgeID += edgeCount;
    _builders.push_back(std::move(builder));
    return *ptr;
}

void ConcurrentWriter::commitAll(JobSystem& jobSystem) {
    JobGroup jobs = jobSystem.newGroup();
    std::vector<std::unique_ptr<DataPart>> parts(_builders.size());
    auto view = _graph->view();

    for (const auto& [i, builder] : _builders | ranges::views::enumerate) {
        const auto [firstNodeID, firstEdgeID] = _graph->allocIDRange(builder->nodeCount(), builder->edgeCount());

        jobs.submit<void>([&jobSystem, &parts, &view, &builder, firstNodeID, firstEdgeID, i](Promise*) {
            parts[i] = std::make_unique<DataPart>(firstNodeID, firstEdgeID);
            parts[i]->load(view, jobSystem, *builder);
            builder.reset();
        });
    }

    jobs.wait();

    for (auto& part : parts) {
        _graph->_parts->store(std::move(part));
    }

    clear();
}

}

