#include "LoadCSVNode.h"

using namespace db;

LoadCSVNode::~LoadCSVNode() = default;

LoadCSVNode::LoadCSVNode(PlanGraphNodeID id,
                         const fs::Path& path,
                         bool hasHeaders,
                         bool skipOnError,
                         const VarDecl* aliasDecl)
    : VarDeclProviderNode(id, PlanGraphOpcode::LOAD_CSV, aliasDecl),
    _path(path),
    _hasHeaders(hasHeaders),
    _skipOnError(skipOnError)
{
}
