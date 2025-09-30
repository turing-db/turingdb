#pragma once

#include <unordered_map>
#include <string_view>

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

private:
    DeclContext* _parent {nullptr};
    std::unordered_map<std::string_view, VarDecl*> _declMap;

    DeclContext(DeclContext* parent);
    ~DeclContext();

    void addDecl(VarDecl* decl);
};

}
