#pragma once

#include <vector>
#include <utility>

#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"

namespace db::v2 {

class CypherAST;
class Expr;

class NodePatternData {
public:
    using ExprConstraints = std::vector<std::pair<PropertyType, Expr*>>;
    friend CypherAST;

    static NodePatternData* create(CypherAST* ast);

    const LabelSet& labelConstraints() const { return _labelConstraints; }
    const ExprConstraints& exprConstraints() { return _exprConstraints; }

    void addLabelConstraint(LabelID labelID);
    void addExprConstraint(PropertyType propType, Expr* expr);

private:
    LabelSet _labelConstraints;
    ExprConstraints _exprConstraints;

    NodePatternData();
    ~NodePatternData();
};

class EdgePatternData {
public:
    using EdgeTypes = std::vector<EdgeTypeID>;
    using ExprConstraints = std::vector<std::pair<PropertyType, Expr*>>;
    friend CypherAST;

    static EdgePatternData* create(CypherAST* ast);

    const EdgeTypes& edgeTypeConstraints() const { return _edgeTypeConstraints; }
    const ExprConstraints& exprConstraints() { return _exprConstraints; }

    void addEdgeTypeConstraint(EdgeTypeID edgeTypeID);
    void addExprConstraint(PropertyType propType, Expr* expr);

private:
    EdgeTypes _edgeTypeConstraints;
    ExprConstraints _exprConstraints;

    EdgePatternData();
    ~EdgePatternData();
};

}
