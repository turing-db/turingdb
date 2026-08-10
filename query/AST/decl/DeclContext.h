#pragma once

#include <vector>
#include <unordered_map>
#include <string_view>

#include "decl/EvaluatedType.h"

namespace db {

class CypherAST;
class VarDecl;

class DeclContext {
public:
    using DeclMap = std::unordered_map<std::string_view, VarDecl*>;
    using Decls = std::vector<VarDecl*>;

    friend CypherAST;
    friend VarDecl;

    static DeclContext* create(CypherAST* ast, DeclContext* parent);

    DeclContext* getParent() const { return _parent; }

    const Decls& decls() const { return _decls; }

    bool hasDecl(std::string_view name) const;

    VarDecl* getDecl(std::string_view name) const;

    VarDecl* getOrCreateNamedVariable(CypherAST* ast, EvaluatedType type, std::string_view name);
    VarDecl* createUnnamedVariable(CypherAST* ast, EvaluatedType type);

    void declareAlias(std::string_view name, VarDecl* decl);

private:
    DeclContext* _parent {nullptr};
    DeclMap _declMap;
    Decls _decls;

    DeclContext(DeclContext* parent);
    ~DeclContext();

    uint64_t _unnamedVarCounter {0};

    void addDecl(VarDecl* decl);
};

}
