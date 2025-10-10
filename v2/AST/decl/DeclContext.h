#pragma once

#include <unordered_map>
#include <string_view>

#include "decl/EvaluatedType.h"

namespace db::v2 {

class CypherAST;
class VarDecl;

class DeclContext {
public:
    friend CypherAST;
    friend VarDecl;

    static DeclContext* create(CypherAST* ast, DeclContext* parent);

    DeclContext* getParent() const { return _parent; }

    VarDecl* getDecl(std::string_view name) const;
    VarDecl* getOrCreateNamedVariable(CypherAST* ast, EvaluatedType type, std::string_view name);
    VarDecl* createUnnamedVariable(CypherAST* ast, EvaluatedType type);

private:
    DeclContext* _parent {nullptr};
    std::unordered_map<std::string_view, VarDecl*> _declMap;

    DeclContext(DeclContext* parent);
    ~DeclContext();

    uint64_t _unnamedVarCounter {0};

    void addDecl(VarDecl* decl);
};

}
