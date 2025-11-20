#include "DeclContext.h"

#include <spdlog/fmt/bundled/format.h>

#include "CompilerException.h"
#include "DiagnosticsManager.h"
#include "CypherAST.h"
#include "VarDecl.h"

using namespace db::v2;

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
    }

    if (decl->getType() != type) {
        std::string msg = fmt::format("Variable '{}' is already declared with type '{}'",
                                      name,
                                      EvaluatedTypeName::value(decl->getType()));
        throw CompilerException(ast->getDiagnosticsManager()->createErrorString(msg, nullptr));
    }

    return decl;
}

VarDecl* DeclContext::createUnnamedVariable(CypherAST* ast, EvaluatedType type) {
    std::string* name = ast->createString();
    name->assign("v" + std::to_string(_unnamedVarCounter++));

    return VarDecl::create(ast, this, *name, type);
}

void DeclContext::addDecl(VarDecl* decl) {
    _declMap[decl->getName()] = decl;
}

