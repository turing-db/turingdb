#pragma once

#include <vector>
#include <utility>

#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"

namespace db::v2 {

class CypherAST;
class Expr;

class PatternData {
public:
    using ExprConstraints = std::vector<std::pair<PropertyType, Expr*>>;

    const ExprConstraints& exprConstraints() const { return _exprConstraints; }

    void addExprConstraint(PropertyType propType, Expr* expr);

protected:
    PatternData();
    virtual ~PatternData();

    ExprConstraints _exprConstraints;
};

class NodePatternData : public PatternData {
public:
    friend CypherAST;

    static NodePatternData* create(CypherAST* ast);

    const LabelSet& labelConstraints() const { return _labelConstraints; }

    void addLabelConstraint(LabelID labelID);

private:
    LabelSet _labelConstraints;

    NodePatternData();
    ~NodePatternData() override;
};

class EdgePatternData : public PatternData {
public:
    using EdgeTypes = std::vector<EdgeTypeID>;
    friend CypherAST;

    static EdgePatternData* create(CypherAST* ast);

    const EdgeTypes& edgeTypeConstraints() const { return _edgeTypeConstraints; }

    void addEdgeTypeConstraint(EdgeTypeID edgeTypeID);

private:
    EdgeTypes _edgeTypeConstraints;

    EdgePatternData();
    ~EdgePatternData() override;
};

}
