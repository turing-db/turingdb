#include <string_view>

#include <spdlog/spdlog.h>

#include "indexes/HAMTIndex.h"
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

        index.exhaustiveMutInsert(key, val);

        const NodeID* n = index.find(key);
        bioassert(n, "find {} failed", key);

        spdlog::info("{}", n->getValue());
    }

    {
        std::string_view key {"my other string"};
        NodeID val {333};

        index.exhaustiveMutInsert(key, val);

        const NodeID* n = index.find(key);
        bioassert(n, "find {} failed", key);

        spdlog::info("{}", n->getValue());
    }

    {
        std::string_view key {"my third string"};
        NodeID val {8333333333333333333};

        index.exhaustiveMutInsert(key, val);

        const NodeID* n = index.find(key);
        bioassert(n, "find {} failed", key);

        spdlog::info("{}", n->getValue());
    }
}
