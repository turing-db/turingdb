#pragma once

#include <vector>

#include "PatternEntity.h"

namespace db {

class PatternNode;
class PatternEdge;

class PatternPart {
public:
    PatternPart() = default;
    ~PatternPart() = default;

    PatternPart(const PatternPart&) = default;
    PatternPart(PatternPart&&) = default;
    PatternPart& operator=(const PatternPart&) = default;
    PatternPart& operator=(PatternPart&&) = default;

    static std::unique_ptr<PatternPart> create() {
        return std::make_unique<PatternPart>();
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

private:
    std::vector<PatternEntity*> _entities;
};

}
