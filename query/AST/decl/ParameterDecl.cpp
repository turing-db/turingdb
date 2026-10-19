#include "ParameterDecl.h"

#include "CypherAST.h"

using namespace db;

ParameterDecl::ParameterDecl(std::string_view name)
    : _name(name)
{
}

ParameterDecl::~ParameterDecl() {
}

ParameterDecl* ParameterDecl::create(CypherAST* ast, std::string_view name) {
    ParameterDecl* decl = new ParameterDecl(name);
    ast->addParameterDecl(decl);

    return decl;
}
