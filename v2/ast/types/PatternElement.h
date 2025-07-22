#pragma once

#include <vector>

#include "PatternEntity.h"
#include "NodePattern.h"
#include "PatternEdge.h"

namespace db {

class NodePattern;
class EdgePattern;

class PatternElement {
public:
    PatternElement() = default;
    ~PatternElement() = default;

    PatternElement(const PatternElement&) = default;
    PatternElement(PatternElement&&) = default;
    PatternElement& operator=(const PatternElement&) = default;
    PatternElement& operator=(PatternElement&&) = default;

    static std::unique_ptr<PatternElement> create() {
        return std::make_unique<PatternElement>();
    }

    void addRootNode(NodePattern* node) {
        _entities.insert(_entities.begin(), (PatternEntity*)node);
    }

    void addRootEdge(EdgePattern* edge) {
        _entities.insert(_entities.begin(), (PatternEntity*)edge);
    }

    void addNode(NodePattern* node) {
        _entities.emplace_back(node);
    }

    void addEdge(EdgePattern* edge) {
        _entities.emplace_back(edge);
    }

    const std::vector<PatternEntity*>& getEntities() const {
        return _entities;
    }

private:
    std::vector<PatternEntity*> _entities;
};

}
