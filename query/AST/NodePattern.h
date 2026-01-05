#pragma once

#include "EntityPattern.h"

namespace db::v2 {

class CypherAST;
class Symbol;
class SymbolChain;
class NodePatternData;
class Expr;

class NodePattern : public EntityPattern {
public:
    static NodePattern* create(CypherAST* ast);
    static NodePattern* fromExpr(CypherAST* ast, Expr* expr);

    const SymbolChain* labels() const { return _labels; }

    NodePatternData* getData() const { return _data; }

    void setLabels(SymbolChain* labels) { _labels = labels; }
    void setData(NodePatternData* data) { _data = data; }

private:
    SymbolChain* _labels {nullptr};
    NodePatternData* _data {nullptr};

    NodePattern();
    ~NodePattern() override;
};

}
