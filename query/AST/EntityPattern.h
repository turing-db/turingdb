#pragma once

namespace db {

class Symbol;
class CypherAST;
class MapLiteral;
class VarDecl;
class WhereClause;

class EntityPattern {
public:
    friend CypherAST;

    Symbol* getSymbol() const { return _symbol; }
    MapLiteral* getProperties() const { return _properties; }
    WhereClause* getWhere() const { return _where; }

    VarDecl* getDecl() const { return _decl; }

    void setSymbol(Symbol* symbol) { _symbol = symbol; }
    void setProperties(MapLiteral* properties) { _properties = properties; }
    void setWhere(WhereClause* where) { _where = where; }

    void setDecl(VarDecl* decl) { _decl = decl; }

protected:
    EntityPattern();
    virtual ~EntityPattern();

private:
    Symbol* _symbol {nullptr};
    MapLiteral* _properties {nullptr};
    WhereClause* _where {nullptr};
    VarDecl* _decl {nullptr};
};

}
