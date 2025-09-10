#pragma once

#include <optional>

#include "Symbol.h"

namespace db::v2 {

class MapLiteral;
class VarDecl;
class AnalysisData;
class CypherAST;

class EntityPattern {
public:
    friend CypherAST;

    static EntityPattern* create(CypherAST* ast);

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

    bool analyzed() const {
        return _data != nullptr;
    }

    const AnalysisData& data() const { return *_data; }

    AnalysisData& data() { return *_data; }

    void setData(AnalysisData* data) {
        _data = data;
    }

protected:
    EntityPattern() = default;
    virtual ~EntityPattern() = default;

private:
    AnalysisData* _data {nullptr};
    std::optional<Symbol> _symbol;
    MapLiteral* _properties {nullptr};
};

}
