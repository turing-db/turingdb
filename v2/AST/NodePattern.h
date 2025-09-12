#pragma once

#include <vector>

#include "EntityPattern.h"

namespace db::v2 {

class CypherAST;
class Symbol;
class NodePatternData;
class Expr;

class NodePattern : public EntityPattern {
public:
    using Labels = std::vector<Symbol*>;

    static NodePattern* create(CypherAST* ast);
    static NodePattern* fromExpr(CypherAST* ast, Expr* expr);

    const Labels& labels() const { return _labels; }

    NodePatternData* getData() const { return _data; }

    void setLabels(Labels&& labels) { _labels = std::move(labels); }
    void setLabels(const Labels& labels) { _labels = labels; }
    void setData(NodePatternData* data) { _data = data; }

private:
    Labels _labels;
    NodePatternData* _data {nullptr};

    NodePattern();
    ~NodePattern() override;
};

}
