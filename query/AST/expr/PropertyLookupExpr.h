#pragma once

#include "Expr.h"

#include "metadata/DateTime.h"

namespace db {

class CypherAST;

// A property read off the value of an expression rather than off a variable:
// startNode(r).name, or datetime('...').year.
class PropertyLookupExpr : public Expr {
public:
    static PropertyLookupExpr* create(CypherAST* ast, Expr* base, std::string_view propName);

    Expr* getBase() const { return _base; }
    std::string_view getPropName() const { return _propName; }

    bool readsADateTimeComponent() const { return _readsADateTimeComponent; }
    DateTimePart getDateTimePart() const { return _dateTimePart; }
    void setDateTimePart(DateTimePart part);

private:
    Expr* _base {nullptr};
    std::string_view _propName;
    DateTimePart _dateTimePart {DateTimePart::Year};
    bool _readsADateTimeComponent {false};

    PropertyLookupExpr(Expr* base, std::string_view propName);

    ~PropertyLookupExpr() override;
};

}
