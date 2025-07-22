#include "DeclContext.h"

#include <spdlog/fmt/bundled/format.h>

#include "ASTException.h"
#include "VarDecl.h"

using namespace db;

DeclContext::DeclContext() = default;

DeclContext::~DeclContext() = default;

DeclContext::DeclContext(DeclContext* parent)
    : _parent(parent) {
}

VarDecl* DeclContext::getOrCreateNamedVariable(std::string_view name, VariableType type) {
    // Search in current scope

    auto it = _vars.find(name);
    if (it != _vars.end()) {
        if (it->second->type() != type) {
            throw ASTException(fmt::format("Variable type mismatch. Variable is of type '{}', first occurrence was '{}'",
                                           VariableTypeName::value(type),
                                           VariableTypeName::value(it->second->type())));
        }

        return it->second.get();
    }

    // Search in parent scope
    if (_parent != nullptr) {
        VarDecl* declPtr = _parent->tryGetDecl(name);

        if (declPtr != nullptr) {
            // Found it, checking type match
            if (declPtr->type() != type) {
                throw ASTException(fmt::format("Variable type mismatch. Variable is of type '{}', first occurrence was '{}'",
                                               VariableTypeName::value(type),
                                               VariableTypeName::value(declPtr->type())));
            }

            return declPtr;
        }
    }

    // Not found anywhere, create it

    auto decl = std::make_unique<VarDecl>(name, type);

    auto* declPtr = decl.get();
    _vars[name] = std::move(decl);

    return declPtr;
}

VarDecl* DeclContext::createUnnamedVariable(VariableType type) {
    // Search in current scope

    auto decl = std::make_unique<VarDecl>(type);
    auto* declPtr = decl.get();

    _unnamedVars.push_back(std::move(decl));

    return declPtr;
}

VarDecl* DeclContext::tryGetDecl(std::string_view name) const {
    auto it = _vars.find(name);
    if (it != _vars.end()) {
        return it->second.get();
    }

    if (_parent != nullptr) {
        return _parent->tryGetDecl(name);
    }

    return nullptr;
}
