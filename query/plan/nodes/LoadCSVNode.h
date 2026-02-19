#pragma once

#include "Path.h"
#include "PlanGraphNode.h"

namespace db {

class VarDecl;

class LoadCSVNode : public PlanGraphNode {
public:
    LoadCSVNode(const fs::Path& path,
                bool hasHeaders,
                bool skipOnError,
                const VarDecl* aliasDecl);

    const fs::Path& getFilePath() const { return _path; }
    bool hasHeaders() const { return _hasHeaders; }
    bool skipOnError() const { return _skipOnError; }
    const VarDecl* getAliasDecl() const { return _aliasDecl; }

private:
    fs::Path _path;
    bool _hasHeaders {false};
    bool _skipOnError {false};
    const VarDecl* _aliasDecl {nullptr};
};

}
