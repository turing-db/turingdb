#include "PropertyLookupExpr.h"

#include "CypherAST.h"

using namespace db;

PropertyLookupExpr::PropertyLookupExpr(Expr* base, std::string_view propName)
    : Expr(Kind::PROPERTY_LOOKUP),
    _base(base),
    _propName(propName)
{
}

PropertyLookupExpr::~PropertyLookupExpr() {
}

PropertyLookupExpr* PropertyLookupExpr::create(CypherAST* ast, Expr* base, std::string_view propName) {
    PropertyLookupExpr* expr = new PropertyLookupExpr(base, propName);
    ast->addExpr(expr);
    return expr;
}

void PropertyLookupExpr::setDateTimePart(DateTimePart part) {
    _dateTimePart = part;
    _readsADateTimeComponent = true;
}
