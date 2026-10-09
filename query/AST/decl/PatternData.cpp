#include "PatternData.h"

#include "CypherAST.h"

using namespace db;

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

void NodePatternData::addLabelConstraints(const SymbolChain* labels) {
    collectSchemaNames(labels, _labelConstraints);
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

void EdgePatternData::addEdgeTypeConstraints(const SymbolChain* edgeTypes) {
    collectSchemaNames(edgeTypes, _edgeTypeConstraints);
}
