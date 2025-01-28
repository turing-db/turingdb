#include "GraphLoadStatus.h"

#include <shared_mutex>
#include <mutex>

using namespace db;

bool GraphLoadStatus::addLoadingGraph(const std::string& graphName) {
    std::unique_lock lock(_guard);

    if (_loadingGraphs.contains(graphName)) {
        return false;
    }

    _loadingGraphs.emplace(graphName);
    return true;
}

void GraphLoadStatus::removeLoadingGraph(const std::string& graphName) {
    std::unique_lock lock(_guard);

    if (!_loadingGraphs.contains(graphName)) {
        return;
    }

    _loadingGraphs.erase(graphName);
}

bool GraphLoadStatus::isGraphLoading(const std::string& graphName) const {
    std::shared_lock lock(_guard);

    return _loadingGraphs.contains(graphName);
} 
