#pragma once

#include <memory>

#include "pattern/PatternEntity.h"

namespace db {

class PatternEdge : public PatternEntity {
public:
    PatternEdge() = default;
    ~PatternEdge() override = default;

    PatternEdge(const PatternEdge&) = default;
    PatternEdge(PatternEdge&&) = default;
    PatternEdge& operator=(const PatternEdge&) = default;
    PatternEdge& operator=(PatternEdge&&) = default;

    static std::unique_ptr<PatternEdge> create() {
        return std::make_unique<PatternEdge>();
    }
};

}
