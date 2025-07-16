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
