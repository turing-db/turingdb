#include "PatternData.h"

#include "CypherAST.h"

using namespace db;

bool LabelRef::operator==(const LabelRef& other) const {
    return _name == other._name && _isParameter == other._isParameter;
}

// PatternData
PatternData::PatternData()
{
}

PatternData::~PatternData() {
}

void PatternData::addExprConstraint(std::string_view typeName, ValueType valueType, Expr* expr) {
    _exprConstraints.emplace_back(typeName, valueType, expr);
}

// NodePatternData
NodePatternData::NodePatternData()
{
}

NodePatternData::~NodePatternData() {
}

NodePatternData* NodePatternData::create(CypherAST* ast) {
    NodePatternData* data = new NodePatternData();
    ast->addNodePatternData(data);
    return data;
}

void NodePatternData::addLabelConstraint(std::string_view label) {
    _labelConstraints.emplace_back(label, false);
}

void NodePatternData::addLabelParameter(std::string_view name) {
    _labelConstraints.emplace_back(name, true);
}

// EdgePatternData
EdgePatternData::EdgePatternData()
{
}

EdgePatternData::~EdgePatternData() {
}

EdgePatternData* EdgePatternData::create(CypherAST* ast) {
    EdgePatternData* data = new EdgePatternData();
    ast->addEdgePatternData(data);
    return data;
}

void EdgePatternData::addEdgeTypeConstraint(std::string_view edgeType) {
    _edgeTypeConstraints.push_back(edgeType);
}
