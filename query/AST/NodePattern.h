#pragma once

#include "EntityPattern.h"

namespace db {

class CypherAST;
class Symbol;
class LabelExpr;
class NodePatternData;
class Expr;

class NodePattern : public EntityPattern {
public:
    static NodePattern* create(CypherAST* ast);
    static NodePattern* fromExpr(CypherAST* ast, Expr* expr);

    LabelExpr* getLabelExpr() const { return _labelExpr; }

    NodePatternData* getData() const { return _data; }

    void setLabelExpr(LabelExpr* labelExpr) { _labelExpr = labelExpr; }
    void setData(NodePatternData* data) { _data = data; }

private:
    LabelExpr* _labelExpr {nullptr};
    NodePatternData* _data {nullptr};

    NodePattern();
    ~NodePattern() override;
};

}
