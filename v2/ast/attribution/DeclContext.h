#pragma once

#include <string_view>
#include <unordered_map>
#include <vector>
#include <memory>

#include "VariableType.h"

namespace db {

class VarDecl;

class DeclContext {
public:
    DeclContext();
    ~DeclContext();

    explicit DeclContext(DeclContext* parent);

    DeclContext(const DeclContext&) = delete;
    DeclContext& operator=(const DeclContext&) = delete;
    DeclContext(DeclContext&&) = delete;
    DeclContext& operator=(DeclContext&&) = delete;

    bool hasParent() const {
        return _parent != nullptr;
    }

    VarDecl* getOrCreateNamedVariable(std::string_view name, VariableType type);
    VarDecl* createUnnamedVariable(VariableType type);
    VarDecl* tryGetDecl(std::string_view name) const;

private:
    DeclContext* _parent {nullptr};
    std::vector<DeclContext*> _children;

    std::vector<std::unique_ptr<VarDecl>> _unnamedVars;
    std::unordered_map<std::string_view, std::unique_ptr<VarDecl>> _vars;
};

}
