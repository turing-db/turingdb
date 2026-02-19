#pragma once

#include "Stmt.h"
#include "Path.h"

namespace db {

class CypherAST;
class Symbol;
class VarDecl;

class LoadCSVStmt : public Stmt {
public:
    static LoadCSVStmt* create(CypherAST* ast,
                               std::string_view filePath,
                               Symbol* alias);

    Kind getKind() const override { return Kind::LOAD_CSV; }

    const fs::Path& getFilePath() const { return _filePath; }
    Symbol* getAlias() const { return _alias; }
    bool hasHeaders() const { return _hasHeaders; }
    bool skipOnError() const { return _skipOnError; }

    VarDecl* getAliasDecl() const { return _aliasDecl; }

    void setHasHeaders(bool hasHeaders) { _hasHeaders = hasHeaders; }
    void setSkipOnError(bool skip) { _skipOnError = skip; }
    void setAliasDecl(VarDecl* decl) { _aliasDecl = decl; }

private:
    fs::Path _filePath;
    Symbol* _alias {nullptr};
    VarDecl* _aliasDecl {nullptr};
    bool _hasHeaders {false};
    bool _skipOnError {false};

    LoadCSVStmt(std::string_view filePath, Symbol* alias);
    ~LoadCSVStmt() override;
};

}
