#pragma once

#include <unordered_map>
#include <string_view>
#include <string>
#include <vector>
#include <memory>

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

    std::string_view storeUnnamedVarName(std::string&& name) {
        auto inserted = std::make_unique<std::string>(std::move(name));
        std::string& toreturn = *inserted;
        _unnamedVarNames.emplace_back(std::move(inserted));

        return toreturn;
    }

private:
    DeclContext* _parent {nullptr};
    std::unordered_map<std::string_view, VarDecl*> _declMap;
    std::vector<std::unique_ptr<std::string>> _unnamedVarNames;

    DeclContext(DeclContext* parent);
    ~DeclContext();

    void addDecl(VarDecl* decl);
};

}
