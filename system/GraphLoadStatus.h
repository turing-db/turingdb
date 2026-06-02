#pragma once

#include <functional>
#include <set>
#include <string>
#include <string_view>

#include "RWSpinLock.h"

namespace db {

class GraphLoadStatus {
public:
    bool addLoadingGraph(std::string_view graphName);
    void removeLoadingGraph(std::string_view graphName);
    bool isGraphLoading(std::string_view graphName) const;

private:
    mutable RWSpinLock _guard;
    std::set<std::string, std::less<>> _loadingGraphs;
};

}
