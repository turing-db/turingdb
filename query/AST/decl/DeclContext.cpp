#include "DeclContext.h"

#include <spdlog/fmt/bundled/format.h>

#include "CompilerException.h"
#include "DiagnosticsManager.h"
#include "CypherAST.h"
#include "VarDecl.h"

using namespace db;

DeclContext::DeclContext(DeclContext* parent)
    : _parent(parent)
{
}

DeclContext::~DeclContext() {
}

DeclContext* DeclContext::create(CypherAST* ast, DeclContext* parent) {
    DeclContext* ctxt = new DeclContext(parent);
    ast->addDeclContext(ctxt);
    return ctxt;
}

bool DeclContext::hasDecl(std::string_view name) const {
    return _declMap.contains(name);
}

VarDecl* DeclContext::getDecl(std::string_view name) const {
    const auto it = _declMap.find(name);
    if (it == _declMap.end()) {
        return nullptr;
    }

    return it->second;
}

VarDecl* DeclContext::getOrCreateNamedVariable(CypherAST* ast, EvaluatedType type, std::string_view name) {
    VarDecl* decl = getDecl(name);
    if (!decl) {
        decl = VarDecl::create(ast, this, name, type);
        decl->setIsUnnamed(false);
        _declMap[decl->getName()] = decl;
    }

    if (decl->getType() != type) {
        const std::string msg = fmt::format("Variable '{}' is already declared with type '{}'",
                                            name,
                                            EvaluatedTypeName::value(decl->getType()));
        std::string errorStr;
        ast->getDiagnosticsManager()->createErrorString(msg, nullptr, errorStr);
        throw CompilerException(std::move(errorStr));
    }

    return decl;
}

VarDecl* DeclContext::getOrCreateNodePatternVariable(CypherAST* ast, std::string_view name) {
    VarDecl* declared = getDecl(name);

    const bool holdsUnwoundIntegers = declared
                                      && declared->getType() == EvaluatedType::Integer
                                      && declared->isUnwound();

    if (holdsUnwoundIntegers) {
        declared->setType(EvaluatedType::NodePattern);

        return declared;
    }

    return getOrCreateNamedVariable(ast, EvaluatedType::NodePattern, name);
}

VarDecl* DeclContext::declareProjectedVariable(CypherAST* ast, EvaluatedType type, std::string_view name) {
    VarDecl* declared = getDecl(name);
    if (declared && declared->getType() == type) {
        return declared;
    }

    VarDecl* decl = VarDecl::create(ast, this, name, type);
    decl->setIsUnnamed(false);
    _declMap[decl->getName()] = decl;

    return decl;
}

VarDecl* DeclContext::createUnnamedVariable(CypherAST* ast, EvaluatedType type) {
    std::string* name = ast->createString();

    // The generated names share one spelling with what a query may call its own
    // variables, and `v0` is a legal Cypher identifier: skipping the ones already
    // declared here keeps a generated name from taking over a name the query wrote
    do {
        name->assign("v" + std::to_string(_unnamedVarCounter++));
    } while (hasDecl(*name));

    VarDecl* decl = VarDecl::create(ast, this, *name, type);
    decl->setIsUnnamed(true);

    return decl;
}

void DeclContext::declareAlias(std::string_view name, VarDecl* decl) {
    // The alias names no new variable, which is why it is bound to an existing declaration
    // and not appended to `_decls`: a wildcard must not return the variable twice
    _declMap[name] = decl;
}

void DeclContext::addDecl(VarDecl* decl) {
    _decls.push_back(decl);
}

