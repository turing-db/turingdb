#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "PatternEntity.h"

namespace db {

class MapLiteral;

class NodePattern : public PatternEntity {
public:
    using LabelVector = std::vector<std::string_view>;

    NodePattern() = default;
    ~NodePattern() override = default;

    NodePattern(const NodePattern&) = default;
    NodePattern(NodePattern&&) = default;
    NodePattern& operator=(const NodePattern&) = default;
    NodePattern& operator=(NodePattern&&) = default;

    static std::unique_ptr<NodePattern> create() {
        return std::make_unique<NodePattern>();
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

    void setLabels(std::optional<LabelVector>&& labels) {
        _labels = std::move(labels);
    }

private:
    std::optional<LabelVector> _labels;
};

}
