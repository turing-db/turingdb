#pragma once

#include <memory>
#include <optional>

#include "types/Symbol.h"
#include "types/PatternEntity.h"

namespace db {

class MapLiteral;

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

    const Symbol& symbol() const {
        return _symbol.value();
    }

    const std::string& type() const {
        return _type.value();
    }

    const MapLiteral& properties() const {
        return *_properties;
    }

    Symbol& symbol() {
        return _symbol.value();
    }

    std::string& type() {
        return _type.value();
    }

    MapLiteral& properties() {
        return *_properties;
    }

    bool hasSymbol() const {
        return _symbol.has_value();
    }

    bool hasType() const {
        return _type.has_value();
    }

    bool hasProperties() const {
        return _properties != nullptr;
    }

    void setSymbol(Symbol&& symbol) {
        _symbol = std::move(symbol);
    }

    void setType(std::string&& type) {
        _type = std::move(type);
    }

    void setProperties(MapLiteral* properties) {
        _properties = properties;
    }



private:
    std::optional<Symbol> _symbol;
    std::optional<std::string> _type;
    MapLiteral* _properties {nullptr};
};

}
