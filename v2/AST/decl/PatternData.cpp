#include "PatternData.h"

#include "CypherAST.h"

using namespace db::v2;

// PatternData
PatternData::PatternData()
{
}

PatternData::~PatternData() {
}

void PatternData::addExprConstraint(PropertyType propType, Expr* expr) {
    _exprConstraints.emplace_back(propType, expr);
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

void NodePatternData::addLabelConstraint(LabelID labelID) {
    _labelConstraints.set(labelID);
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

void EdgePatternData::addEdgeTypeConstraint(EdgeTypeID edgeTypeID) {
    _edgeTypeConstraints.push_back(edgeTypeID);
}
