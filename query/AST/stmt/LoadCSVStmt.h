#pragma once

#include <span>
#include <string_view>
#include <vector>

#include "Stmt.h"
#include "Path.h"

namespace db {

class CypherAST;
class Symbol;
class VarDecl;

class LoadCSVStmt : public Stmt {
public:
    // One field of the row the statement loads: the position `row[2]` names, or the
    // header `row.age` names. The load exposes one column per field, so the fields are
    // held by the statement rather than by each access, and every access to the same
    // field reads the one column - through @ref _decl, which every access naming that
    // field shares.
    struct Field {
        std::string_view _header;
        size_t _index {0};
        bool _byHeader {false};
        VarDecl* _decl {nullptr};
    };

    using Fields = std::vector<Field>;

    static LoadCSVStmt* create(CypherAST* ast,
                               std::string_view filePath,
                               Symbol* alias);

    Kind getKind() const override { return Kind::LOAD_CSV; }

    const fs::Path& getFilePath() const { return _filePath; }
    Symbol* getAlias() const { return _alias; }
    bool hasHeaders() const { return _hasHeaders; }
    bool skipOnError() const { return _skipOnError; }

    VarDecl* getAliasDecl() const { return _aliasDecl; }

    std::span<const Field> fields() const { return _fields; }
    const Field& getField(size_t slot) const { return _fields[slot]; }

    void setHasHeaders(bool hasHeaders) { _hasHeaders = hasHeaders; }
    void setSkipOnError(bool skip) { _skipOnError = skip; }
    void setAliasDecl(VarDecl* decl) { _aliasDecl = decl; }
    void setFieldDecl(size_t slot, VarDecl* decl) { _fields[slot]._decl = decl; }

    // The slot of the field a positional access names, adding it when this access is
    // the first to reach it, so the load exposes one column however many times the
    // query reads the field
    size_t declareField(size_t index);

    // The header sibling of the positional overload
    size_t declareField(std::string_view header);

private:
    fs::Path _filePath;
    Symbol* _alias {nullptr};
    VarDecl* _aliasDecl {nullptr};
    Fields _fields;
    bool _hasHeaders {false};
    bool _skipOnError {false};

    LoadCSVStmt(std::string_view filePath, Symbol* alias);
    ~LoadCSVStmt() override;
};

}
