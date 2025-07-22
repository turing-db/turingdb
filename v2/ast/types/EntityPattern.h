#pragma once

#include <memory>
#include <optional>

#include "Symbol.h"

namespace db {

class MapLiteral;

class EntityPattern {
public:
    EntityPattern() = default;
    virtual ~EntityPattern() = default;

    EntityPattern(const EntityPattern&) = default;
    EntityPattern(EntityPattern&&) = default;
    EntityPattern& operator=(const EntityPattern&) = default;
    EntityPattern& operator=(EntityPattern&&) = default;

    static std::unique_ptr<EntityPattern> create() {
        return std::make_unique<EntityPattern>();
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
