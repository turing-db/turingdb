#pragma once

#include <set>
#include <string>

#include "RWSpinLock.h"

namespace db {

class GraphLoadStatus {
public:
    bool addLoadingGraph(const std::string& graphName);
    void removeLoadingGraph(const std::string& graphName);
    bool isGraphLoading(const std::string& graphName) const;

private:
    mutable RWSpinLock _guard;
    std::set<std::string> _loadingGraphs;
};

} 
