#include "GraphLoadStatus.h"

#include <shared_mutex>
#include <mutex>

using namespace db;

bool GraphLoadStatus::addLoadingGraph(std::string_view graphName) {
    std::unique_lock lock(_guard);

    if (_loadingGraphs.contains(graphName)) {
        return false;
    }

    _loadingGraphs.emplace(graphName);
    return true;
}

void GraphLoadStatus::removeLoadingGraph(std::string_view graphName) {
    std::unique_lock lock(_guard);

    const auto it = _loadingGraphs.find(graphName);
    if (it != _loadingGraphs.end()) {
        _loadingGraphs.erase(it);
    }
}

bool GraphLoadStatus::isGraphLoading(std::string_view graphName) const {
    std::shared_lock lock(_guard);

    return _loadingGraphs.contains(graphName);
}
