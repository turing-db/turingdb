#pragma once

#include "EntityPattern.h"
#include "expr/MapProjectionExpr.h"

namespace db {

class CypherAST;
class Symbol;
class LabelExpr;
class MapLiteral;
class NodePatternData;
class Expr;

class NodePattern : public EntityPattern {
public:
    static NodePattern* create(CypherAST* ast);
    static NodePattern* fromExpr(CypherAST* ast, Expr* expr);

    // `(n {key: value})` reads as a map projection wherever an expression may stand. As a
    // node pattern its entries are the node's properties, and any other item is no property.
    static MapLiteral* createProperties(CypherAST* ast, const MapProjectionExpr::Items& items);

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
