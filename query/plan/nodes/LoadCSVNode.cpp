#include "LoadCSVNode.h"

using namespace db;

LoadCSVNode::LoadCSVNode(const fs::Path& path,
                         bool hasHeaders,
                         bool skipOnError,
                         const VarDecl* aliasDecl)
    : PlanGraphNode(PlanGraphOpcode::LOAD_CSV),
    _path(path),
    _hasHeaders(hasHeaders),
    _skipOnError(skipOnError),
    _aliasDecl(aliasDecl)
{
}
