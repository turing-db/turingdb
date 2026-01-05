#pragma once

#include <vector>

#include "metadata/PropertyType.h"

namespace db::v2 {

class CypherAST;
class Expr;

struct EntityPropertyConstraint {
    std::string_view _propTypeName;
    ValueType _valueType;
    Expr* _expr {nullptr};
};

class PatternData {
public:
    using ExprConstraints = std::vector<EntityPropertyConstraint>;

    const ExprConstraints& exprConstraints() const { return _exprConstraints; }

    void addExprConstraint(std::string_view typeName, ValueType valueType, Expr* expr);

protected:
    PatternData();
    virtual ~PatternData();

    ExprConstraints _exprConstraints;
};

class NodePatternData : public PatternData {
public:
    friend CypherAST;

    static NodePatternData* create(CypherAST* ast);

    std::span<const std::string_view> labelConstraints() const { return _labelConstraints; }

    void addLabelConstraint(std::string_view label);

private:
    std::vector<std::string_view> _labelConstraints;

    NodePatternData();
    ~NodePatternData() override;
};

class EdgePatternData : public PatternData {
public:
    friend CypherAST;

    static EdgePatternData* create(CypherAST* ast);

    std::span<const std::string_view> edgeTypeConstraints() const { return _edgeTypeConstraints; }

    void addEdgeTypeConstraint(std::string_view edgeType);

private:
    std::vector<std::string_view> _edgeTypeConstraints;

    EdgePatternData();
    ~EdgePatternData() override;
};

}
