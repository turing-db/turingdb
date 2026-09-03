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

    // The name a node of a pattern binds. An integer already in scope is the node IDs an
    // UNWIND bound, and a pattern naming it seeds itself from those nodes, so the variable
    // becomes the pattern node it is used as rather than conflicting with it
    VarDecl* getOrCreateNodePatternVariable(CypherAST* ast, std::string_view name);

    // The name a projected item publishes its column under. What follows the projection
    // reads that column, so the name binds to the item even when the query already used
    // it for a variable of another type
    VarDecl* declareProjectedVariable(CypherAST* ast, EvaluatedType type, std::string_view name);

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
