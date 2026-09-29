#pragma once

#include "Expr.h"

#include "metadata/DateTime.h"
#include "metadata/Duration.h"

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

    bool readsADurationComponent() const { return _readsADurationComponent; }
    DurationPart getDurationPart() const { return _durationPart; }
    void setDurationPart(DurationPart part);

private:
    Expr* _base {nullptr};
    std::string_view _propName;
    DateTimePart _dateTimePart {DateTimePart::Year};
    DurationPart _durationPart {DurationPart::Years};
    bool _readsADateTimeComponent {false};
    bool _readsADurationComponent {false};

    PropertyLookupExpr(Expr* base, std::string_view propName);

    ~PropertyLookupExpr() override;
};

}
