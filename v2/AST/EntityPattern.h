#pragma once

namespace db::v2 {

class Symbol;
class CypherAST;
class MapLiteral;
class VarDecl;

class EntityPattern {
public:
    friend CypherAST;

    Symbol* getSymbol() const { return _symbol; }
    MapLiteral* getProperties() const { return _properties; }

    VarDecl* getDecl() const { return _decl; }

    void setSymbol(Symbol* symbol) { _symbol = symbol; }
    void setProperties(MapLiteral* properties) { _properties = properties; }

    void setDecl(VarDecl* decl) { _decl = decl; }

protected:
    EntityPattern();
    virtual ~EntityPattern();

private:
    Symbol* _symbol {nullptr};
    MapLiteral* _properties {nullptr};
    VarDecl* _decl {nullptr};
};

}
