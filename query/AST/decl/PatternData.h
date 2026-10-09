#pragma once

#include <span>
#include <string_view>
#include <vector>

#include "metadata/PropertyType.h"

namespace db {

class CypherAST;
class Expr;

struct EntityPropertyConstraint {
    std::string_view _propTypeName;
    ValueType _valueType {ValueType::Invalid};
    Expr* _expr {nullptr};
};

// A label a pattern asks for: a name the query wrote, or the parameter the name is bound
// from when the program runs
struct LabelRef {
    std::string_view _name;
    bool _isParameter {false};

    bool operator==(const LabelRef& other) const;
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

    std::span<const LabelRef> labelConstraints() const { return _labelConstraints; }

    void addLabelConstraint(std::string_view label);
    void addLabelParameter(std::string_view name);

private:
    std::vector<LabelRef> _labelConstraints;

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
