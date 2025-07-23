#pragma once

#include <string_view>
#include <unordered_map>
#include <vector>

#include "attribution/DeclID.h"
#include "attribution/VariableType.h"
#include "attribution/VariableDecl.h"

namespace db {

class DeclContainer;

class DeclContext {
public:
    DeclContext(DeclContainer* container, DeclContext* parent = nullptr);
    ~DeclContext();

    DeclContext(const DeclContext&) = delete;
    DeclContext& operator=(const DeclContext&) = delete;
    DeclContext(DeclContext&&) = delete;
    DeclContext& operator=(DeclContext&&) = delete;

    bool hasParent() const {
        return _parent != nullptr;
    }

    VariableDecl tryGetVariable(std::string_view name) const;
    VariableDecl getVariable(std::string_view name) const;
    VariableDecl getUnnamedVariable(DeclID id) const;
    bool hasVariable(std::string_view name) const;

    VariableDecl getOrCreateNamedVariable(std::string_view name, VariableType type);
    VariableDecl createNamedVariable(std::string_view name, VariableType type);
    VariableDecl createUnnamedVariable(VariableType type);

private:
    DeclContainer* _container {nullptr};
    DeclContext* _parent {nullptr};
    std::vector<DeclContext*> _children;

    std::unordered_map<std::string_view, DeclID> _decls;
};

}
