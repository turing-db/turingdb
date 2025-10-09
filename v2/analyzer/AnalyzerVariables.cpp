#include "AnalyzerVariables.h"

#include <spdlog/fmt/bundled/format.h>

#include "AnalyzeException.h"
#include "CypherAST.h"
#include "decl/DeclContext.h"
#include "decl/VarDecl.h"

using namespace db::v2;

AnalyzerVariables::AnalyzerVariables(CypherAST* ast)
    : _ast(ast)
{
}

AnalyzerVariables::~AnalyzerVariables() {
}

VarDecl* AnalyzerVariables::getOrCreateNamedVariable(EvaluatedType type, std::string_view name) {
    if (!_ctxt) [[unlikely]] {
        throwError("Variable declaration context is not set");
    }

    VarDecl* decl = _ctxt->getDecl(name);
    if (!decl) {
        decl = VarDecl::create(_ast, _ctxt, name, type);
    }

    if (decl->getType() != type) {
        throwError(fmt::format("Variable '{}' is already declared with type '{}'",
                               name,
                               EvaluatedTypeName::value(decl->getType())));
    }

    return decl;
}

VarDecl* AnalyzerVariables::createUnnamedVariable(EvaluatedType type) {
    if (!_ctxt) [[unlikely]] {
        throwError("Variable declaration context is not set");
    }

    std::string* name = _ast->createString();
    name->assign("v" + std::to_string(_unnamedVarCounter++));

    return VarDecl::create(_ast, _ctxt, *name, type);
}

void AnalyzerVariables::throwError(std::string_view msg, const void* obj) const {
    throw AnalyzeException(_ast->createErrorString(msg, obj));
}

VarDecl* AnalyzerVariables::getDecl(std::string_view name) const {
    if (!_ctxt) [[unlikely]] {
        throwError("Variable declaration context is not set");
    }

    return _ctxt->getDecl(name);
}
