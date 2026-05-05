#include "indexes/HAMTIndex.h"

#include <string_view>

#include <spdlog/spdlog.h>

#include "indexes/HAMTIndexManager.h"

using namespace db;

int main() {
    GraphView view;
    HAMTManager man;
    PropertyTypeID pid {1};

    HAMTIndex<std::string_view, NodeID> index("my index", &man, pid);
    index.init(view);

    {
        std::string_view key {"my string"};
        NodeID val {101};

        index.mutableInsert(key, val);

        const NodeID* n = index.find(key);

        spdlog::info("{}", n->getValue());
    }

    {
        std::string_view key {"my other string"};
        NodeID val {333};

        index.mutableInsert(key, val);

        const NodeID* n = index.find(key);

        spdlog::info("{}", n->getValue());
    }
}
