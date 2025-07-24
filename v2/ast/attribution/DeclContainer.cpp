#include "attribution/DeclContainer.h"
#include "attribution/VariableDecl.h"

using namespace db;

DeclContainer::DeclContainer() = default;

DeclContainer::~DeclContainer() = default;

VariableDecl& DeclContainer::newDecl(EvaluatedType type) {
    const auto id = _nextID++;
    auto& decl = _decls.emplace_back(std::make_unique<VariableDecl>(type, id));

    return *decl;
}

VariableDecl& DeclContainer::newDecl(EvaluatedType type, std::string_view name) {
    const auto id = _nextID++;
    auto& decl = _decls.emplace_back(std::make_unique<VariableDecl>(type, id));
    decl->setName(name);

    return *decl;
}
