#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "Accessor.h"

#include "GraphFileType.h"
#include "Path.h"

namespace db {

class SystemManager;
class Graph;

class SystemAccessor : public Accessor {
public:
    friend SystemManager;

    SystemAccessor(const SystemAccessor&) = delete;
    SystemAccessor& operator=(const SystemAccessor&) = delete;

    ~SystemAccessor();

    // Graph access
    Graph* getDefaultGraph() const;
    Graph* getGraph(std::string_view name) const;
    size_t getGraphCount() const;

    // Create graph
    Graph* createGraph(std::string_view graphName);

    // List graphs
    void listGraphs(std::vector<std::string_view>& names) const;
    void listAvailableGraphs(std::vector<std::string>& names) const;

    // Load graph
    Graph* loadGraph(std::string_view name);
    bool isGraphLoading(std::string_view name) const;

    // Import graph
    Graph* importGraph(const fs::Path& path, std::string_view name);

    GraphFileType getGraphFileType(const fs::Path& path) const;

private:
    SystemManager* _sysMan {nullptr};

    SystemAccessor(SystemManager* sysMan, SharedAccess);
    SystemAccessor(SystemManager* sysMan, UniqueAccess);
};

}
