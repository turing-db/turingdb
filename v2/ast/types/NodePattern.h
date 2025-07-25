#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "EntityPattern.h"

namespace db {

class MapLiteral;

struct NodePatternData;

class NodePattern : public EntityPattern {
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

    bool hasLabels() const {
        return _labels.has_value();
    }

    bool hasDecl() const {
        return _decl != nullptr;
    }

    bool hasData() const {
        return _data != nullptr;
    }

    const LabelVector& labels() const {
        return _labels.value();
    }

    LabelVector& labels() {
        return _labels.value();
    }

    const VarDecl& decl() const {
        return *_decl;
    }

    const NodePatternData& data() const {
        return *_data;
    }

    void setLabels(std::optional<LabelVector>&& labels) {
        _labels = std::move(labels);
    }

    void setDecl(const VarDecl* decl) {
        _decl = decl;
    }

    void setData(NodePatternData* data) {
        _data = data;
    }

private:
    std::optional<LabelVector> _labels;
    const VarDecl* _decl {nullptr};
    NodePatternData* _data {nullptr};
};

}
