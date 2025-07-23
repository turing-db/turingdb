#include "DeclContext.h"

#include <spdlog/fmt/bundled/format.h>

#include "ASTException.h"
#include "attribution/DeclContainer.h"
#include "attribution/VariableDecl.h"

using namespace db;

DeclContext::DeclContext(DeclContainer* container, DeclContext* parent)
    : _container(container),
      _parent(parent)
{
}

DeclContext::~DeclContext() = default;

VariableDecl DeclContext::tryGetVariable(std::string_view name) const {
    auto it = _decls.find(name);
    if (it != _decls.end()) {
        VariableDecl decl(*_container, it->second);
    }

    if (_parent != nullptr) {
        return _parent->tryGetVariable(name);
    }

    return VariableDecl {*_container};
}

VariableDecl DeclContext::getVariable(std::string_view name) const {
    auto it = _decls.find(name);
    if (it != _decls.end()) {
        return VariableDecl(*_container, it->second);
    }

    if (_parent != nullptr) {
        return _parent->getVariable(name);
    }

    throw ASTException(fmt::format("Variable '{}' not found", name));
}

VariableDecl DeclContext::getUnnamedVariable(DeclID id) const {
    if (!id.valid()) {
        throw ASTException("Variable does not exist (ID is invalid)");
    }

    return VariableDecl(*_container, id);
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

VariableDecl DeclContext::getOrCreateNamedVariable(std::string_view name, VariableType type) {
    // Search in current scope

    auto it = _decls.find(name);
    if (it != _decls.end()) {
        const VariableType typeFirstOccurrence = _container->getType(it->second);

        if (type != typeFirstOccurrence) {
            throw ASTException(fmt::format("Variable type mismatch. Variable is of type '{}', first occurrence was '{}'",
                                           VariableTypeName::value(type),
                                           VariableTypeName::value(typeFirstOccurrence)));
        }

        return VariableDecl (*_container, it->second);
    }

    // Search in parent scope
    if (_parent != nullptr) {
        const VariableDecl decl = _parent->tryGetVariable(name);

        if (decl.valid()) {
            // Found it, checking type match
            if (decl.type() != type) {
                throw ASTException(fmt::format("Variable type mismatch. Variable is of type '{}', first occurrence was '{}'",
                                               VariableTypeName::value(type),
                                               VariableTypeName::value(decl.type())));
            }

            return decl;
        }
    }

    // Not found anywhere, create it
    const DeclID id = _container->newVariable(type, name);
    _decls.emplace(name, id);

    return VariableDecl(*_container, id);
}

VariableDecl DeclContext::createNamedVariable(std::string_view name, VariableType type) {
    if (_decls.contains(name)) {
        throw ASTException(fmt::format("Variable '{}' already exists", name));
    }

    const DeclID id = _container->newVariable(type, name);
    _decls.emplace(name, id);

    return VariableDecl(*_container, id);
}

VariableDecl DeclContext::createUnnamedVariable(VariableType type) {
    return VariableDecl(*_container, _container->newVariable(type));
}


