#pragma once

#include "Path.h"
#include "nodes/VarDeclProviderNode.h"

namespace db {

class VarDecl;

class LoadCSVNode : public VarDeclProviderNode {
public:
    LoadCSVNode(const fs::Path& path,
                bool hasHeaders,
                bool skipOnError,
                const VarDecl* aliasDecl);

    ~LoadCSVNode() override;

    const fs::Path& getFilePath() const { return _path; }
    bool hasHeaders() const { return _hasHeaders; }
    bool skipOnError() const { return _skipOnError; }
    const VarDecl* getAliasDecl() const { return _varDecl; }

private:
    fs::Path _path;
    bool _hasHeaders {false};
    bool _skipOnError {false};
};

}
