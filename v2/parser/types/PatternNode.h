#pragma once

#include <memory>
#include <optional>
#include <vector>

#include "Symbol.h"
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

    const Symbol& symbol() const {
        return _symbol.value();
    }

    const std::vector<std::string_view>& labels() const {
        return _labels.value();
    }

    const MapLiteral& properties() const {
        return *_properties;
    }

    Symbol& symbol() {
        return _symbol.value();
    }

    std::vector<std::string_view>& labels() {
        return _labels.value();
    }

    MapLiteral& properties() {
        return *_properties;
    }

    bool hasSymbol() const {
        return _symbol.has_value();
    }

    bool hasLabels() const {
        return _labels.has_value();
    }

    bool hasProperties() const {
        return _properties != nullptr;
    }

    void setSymbol(const std::optional<Symbol>& symbol) {
        _symbol = symbol;
    }

    void setLabels(std::optional<std::vector<std::string_view>>&& labels) {
        _labels = std::move(labels);
    }

    void setProperties(MapLiteral* properties) {
        _properties = properties;
    }

private:
    std::optional<Symbol> _symbol;
    std::optional<std::vector<std::string_view>> _labels;
    MapLiteral* _properties {nullptr};
};

}
