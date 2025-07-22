#pragma once

#include <vector>

#include "PatternEntity.h"
#include "PatternNode.h"
#include "PatternEdge.h"

namespace db {

class PatternNode;
class PatternEdge;

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

    void addRootNode(PatternNode* node) {
        _entities.insert(_entities.begin(), (PatternEntity*)node);
    }

    void addRootEdge(PatternEdge* edge) {
        _entities.insert(_entities.begin(), (PatternEntity*)edge);
    }

    void addNode(PatternNode* node) {
        _entities.emplace_back(node);
    }

    void addEdge(PatternEdge* edge) {
        _entities.emplace_back(edge);
    }

    const std::vector<PatternEntity*>& getEntities() const {
        return _entities;
    }

private:
    std::vector<PatternEntity*> _entities;
};

}
