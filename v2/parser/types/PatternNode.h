#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "PatternEntity.h"

namespace db {

class MapLiteral;

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

    const std::vector<std::string_view>& labels() const {
        return _labels.value();
    }

    std::vector<std::string_view>& labels() {
        return _labels.value();
    }

    bool hasLabels() const {
        return _labels.has_value();
    }

    void setLabels(std::optional<std::vector<std::string_view>>&& labels) {
        _labels = std::move(labels);
    }

private:
    std::optional<std::vector<std::string_view>> _labels;
};

}
