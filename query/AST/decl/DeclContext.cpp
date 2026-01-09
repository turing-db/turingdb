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

VarDecl* DeclContext::createUnnamedVariable(CypherAST* ast, EvaluatedType type) {
    std::string* name = ast->createString();
    name->assign("v" + std::to_string(_unnamedVarCounter++));
    VarDecl* decl = VarDecl::create(ast, this, *name, type);
    decl->setIsUnnamed(true);

    return decl;
}

void DeclContext::addDecl(VarDecl* decl) {
    _declMap[decl->getName()] = decl;
    _decls.push_back(decl);
}

