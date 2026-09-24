#pragma once

#include "Expr.h"

#include "metadata/PropertyType.h"

namespace db {

class QualifiedName;
class CypherAST;
class VarDecl;

class PropertyExpr : public Expr {
public:
    const QualifiedName* getFullName() const { return _fullName; }

    static PropertyExpr* create(CypherAST* ast, QualifiedName* name);

    VarDecl* getEntityVarDecl() const { return _entityDecl; }

    std::string_view getPropName() const { return _propName; }

    void setEntityVarDecl(VarDecl* entityDecl) { _entityDecl = entityDecl; }

    void setPropertyName(std::string_view propName) { _propName = propName; }

    // The value type the query will create the property with, for a name the graph does
    // not carry yet - a CREATE in this same query spells it out. Invalid for every name
    // the graph already knows, whose type the schema answers for
    ValueType getCreatedValueType() const { return _createdValueType; }
    void setCreatedValueType(ValueType valueType) { _createdValueType = valueType; }

    bool isStringTableHeaderAccess() const { return _stringTableHeaderAccess; }
    void setStringTableHeaderAccess(bool csvHeaderAccess) {
        _stringTableHeaderAccess = csvHeaderAccess;
    }

    // A calendar field read off an instant - d.year, or n.createdAt.year, whose property
    // name is still createdAt. The part is meaningless unless this says so.
    bool readsADateTimeComponent() const { return _readsADateTimeComponent; }
    DateTimePart getDateTimePart() const { return _dateTimePart; }
    void setDateTimePart(DateTimePart part);

    // The field of the loaded row a header access reads, under the declaration the load
    // publishes its column with. Null on a property access, and on a header access whose
    // row no load bound. Kept apart from the expression's own declaration, since an alias
    // on the item replaces that one.
    VarDecl* getCSVFieldDecl() const { return _csvFieldDecl; }
    void setCSVFieldDecl(VarDecl* decl) { _csvFieldDecl = decl; }

private:
    QualifiedName* _fullName {nullptr};
    VarDecl* _entityDecl {nullptr};
    VarDecl* _csvFieldDecl {nullptr};
    std::string_view _propName;
    ValueType _createdValueType {ValueType::Invalid};
    DateTimePart _dateTimePart {DateTimePart::Year};
    bool _stringTableHeaderAccess {false};
    bool _readsADateTimeComponent {false};

    PropertyExpr(QualifiedName* name)
        : Expr(Kind::PROPERTY),
        _fullName(name)
    {
    }

    ~PropertyExpr() override;
};

}
