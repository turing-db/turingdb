#pragma once

#include <memory>
#include <span>
#include <vector>

namespace db {

class PlanGraphNode;

class PlanGraphNodeManager {
public:
    template <typename T, typename... Args>
    T* create(Args&&... args) {
        auto node = std::make_unique<T>(_nextID, std::forward<Args>(args)...);
        _nextID++;
        auto* nodePtr = node.get();
        _nodes.emplace_back(std::move(node));

        return nodePtr;
    }

    std::span<const std::unique_ptr<PlanGraphNode>> nodes() const { return _nodes; }

    void removeIsolatedNodes();

    size_t size() const { return _nodes.size(); }

private:
    std::vector<std::unique_ptr<PlanGraphNode>> _nodes;

    uint64_t _nextID {0};
};

}
