#pragma once

#include <span>
#include <string_view>
#include <vector>

#include "metadata/PropertyType.h"

#include "SchemaNameRef.h"

namespace db {

class CypherAST;
class Expr;
class SymbolChain;

struct EntityPropertyConstraint {
    std::string_view _propTypeName;
    ValueType _valueType {ValueType::Invalid};
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

    std::span<const LabelRef> labelConstraints() const { return _labelConstraints; }

    void addLabelConstraints(const SymbolChain* labels);

private:
    std::vector<LabelRef> _labelConstraints;

    NodePatternData();
    ~NodePatternData() override;
};

class EdgePatternData : public PatternData {
public:
    friend CypherAST;

    static EdgePatternData* create(CypherAST* ast);

    std::span<const EdgeTypeRef> edgeTypeConstraints() const { return _edgeTypeConstraints; }

    void addEdgeTypeConstraints(const SymbolChain* edgeTypes);

private:
    std::vector<EdgeTypeRef> _edgeTypeConstraints;

    EdgePatternData();
    ~EdgePatternData() override;
};

}
