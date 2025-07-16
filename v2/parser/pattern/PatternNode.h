#pragma once

#include <memory>

#include "pattern/PatternEntity.h"

namespace db {

class PatternNode : public PatternEntity {
public:
    PatternNode() = default;
    ~PatternNode() override = default;

    PatternNode(const PatternNode&) = default;
    PatternNode(PatternNode&&) = default;
    PatternNode& operator=(const PatternNode&) = default;
    PatternNode& operator=(PatternNode&&) = default;

    static std::unique_ptr<PatternNode> create() {
        return std::make_unique<PatternNode>();
    }
};

}
