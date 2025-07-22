#pragma once

#include <memory>
#include <optional>

#include "Symbol.h"

namespace db {

class MapLiteral;

class PatternEntity {
public:
    PatternEntity() = default;
    virtual ~PatternEntity() = default;

    PatternEntity(const PatternEntity&) = default;
    PatternEntity(PatternEntity&&) = default;
    PatternEntity& operator=(const PatternEntity&) = default;
    PatternEntity& operator=(PatternEntity&&) = default;

    static std::unique_ptr<PatternEntity> create() {
        return std::make_unique<PatternEntity>();
    }

    const Symbol& symbol() const {
        return _symbol.value();
    }


    const MapLiteral& properties() const {
        return *_properties;
    }

    Symbol& symbol() {
        return _symbol.value();
    }


    MapLiteral& properties() {
        return *_properties;
    }

    bool hasSymbol() const {
        return _symbol.has_value();
    }


    bool hasProperties() const {
        return _properties != nullptr;
    }

    void setSymbol(const std::optional<Symbol>& symbol) {
        _symbol = symbol;
    }


    void setProperties(MapLiteral* properties) {
        _properties = properties;
    }

private:
    std::optional<Symbol> _symbol;
    MapLiteral* _properties {nullptr};
};

}
