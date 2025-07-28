#include "DeclContext.h"

#include <spdlog/fmt/bundled/format.h>

#include "ASTException.h"
#include "attribution/DeclContainer.h"
#include "attribution/VarDecl.h"

using namespace db;

DeclContext::DeclContext(DeclContainer& container, DeclContext* parent)
    : _container(container),
      _parent(parent)
{
}

DeclContext::~DeclContext() = default;

const VarDecl* DeclContext::tryGetVariable(std::string_view name) const {
    auto it = _decls.find(name);
    if (it != _decls.end()) {
        return it->second;
    }

    if (_parent != nullptr) {
        return _parent->tryGetVariable(name);
    }

    return nullptr; // Invalid
}

VarDecl* DeclContext::tryGetVariable(std::string_view name) {
    auto it = _decls.find(name);
    if (it != _decls.end()) {
        return it->second;
    }

    if (_parent != nullptr) {
        return _parent->tryGetVariable(name);
    }

    return nullptr; // Invalid
}

const VarDecl& DeclContext::getVariable(std::string_view name) const {
    auto it = _decls.find(name);
    if (it != _decls.end()) {
        return *it->second;
    }

    if (_parent != nullptr) {
        return _parent->getVariable(name);
    }

    throw ASTException(fmt::format("Variable '{}' not found", name));
}

VarDecl& DeclContext::getVariable(std::string_view name) {
    auto it = _decls.find(name);
    if (it != _decls.end()) {
        return *it->second;
    }

    if (_parent != nullptr) {
        return _parent->getVariable(name);
    }

    throw ASTException(fmt::format("Variable '{}' not found", name));
}

bool DeclContext::hasVariable(std::string_view name) const {
    auto it = _decls.find(name);
    if (it != _decls.end()) {
        return true;
    }

    if (_parent != nullptr) {
        return _parent->hasVariable(name);
    }

    return false;
}

VarDecl& DeclContext::getOrCreateNamedVariable(EvaluatedType type, std::string_view name) {
    // Search in current scope

    auto it = _decls.find(name);
    if (it != _decls.end()) {
        const EvaluatedType typeFirstOccurrence = it->second->type();

        if (type != typeFirstOccurrence) {
            throw ASTException(fmt::format("Variable type mismatch. Variable is of type '{}', first occurrence was '{}'",
                                           EvaluatedTypeName::value(type),
                                           EvaluatedTypeName::value(typeFirstOccurrence)));
        }

        return *it->second;
    }

    // Search in parent scope
    if (_parent != nullptr) {
        VarDecl* decl = _parent->tryGetVariable(name);

        if (decl) {
            // Found it, checking type match
            if (decl->type() != type) {
                throw ASTException(fmt::format("Variable type mismatch. Variable is of type '{}', first occurrence was '{}'",
                                               EvaluatedTypeName::value(type),
                                               EvaluatedTypeName::value(decl->type())));
            }

            return *decl;
        }
    }

    // Not found anywhere, create it
    VarDecl& decl = _container.newDecl(type, name);
    _decls.emplace(name, &decl);

    return decl;
}

VarDecl& DeclContext::createNamedVariable(EvaluatedType type, std::string_view name) {
    if (_decls.contains(name)) {
        throw ASTException(fmt::format("Variable '{}' already exists", name));
    }

    VarDecl& decl = _container.newDecl(type, name);
    _decls.emplace(name, &decl);

    return decl;
}

VarDecl& DeclContext::createUnnamedVariable(EvaluatedType type) {
    return _container.newDecl(type);
}


