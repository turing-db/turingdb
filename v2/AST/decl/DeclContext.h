#pragma once

#include <string_view>
#include <unordered_map>
#include <vector>

#include "decl/DeclID.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"

namespace db {

class DeclContainer;

class DeclContext {
public:
    DeclContext(DeclContainer& container, DeclContext* parent = nullptr);
    ~DeclContext();

    DeclContext(const DeclContext&) = delete;
    DeclContext& operator=(const DeclContext&) = delete;
    DeclContext(DeclContext&&) = delete;
    DeclContext& operator=(DeclContext&&) = delete;

    bool hasParent() const {
        return _parent != nullptr;
    }

    const VarDecl* tryGetVariable(std::string_view name) const;
    VarDecl* tryGetVariable(std::string_view name);
    const VarDecl& getVariable(std::string_view name) const;
    VarDecl& getVariable(std::string_view name);
    const VarDecl& getUnnamedVariable(DeclID id) const;
    bool hasVariable(std::string_view name) const;

    VarDecl& getOrCreateNamedVariable(EvaluatedType type, std::string_view name);
    VarDecl& createNamedVariable(EvaluatedType type, std::string_view name);
    VarDecl& createUnnamedVariable(EvaluatedType type);

private:
    DeclContainer& _container;
    DeclContext* _parent {nullptr};
    std::vector<DeclContext*> _children;

    std::unordered_map<std::string_view, VarDecl*> _decls;
};

}
