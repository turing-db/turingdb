#include "DeclContext.h"

#include "CypherAST.h"
#include "VarDecl.h"

using namespace db::v2;

DeclContext::DeclContext(DeclContext* parent)
    : _parent(parent)
{
}

DeclContext::~DeclContext() {
}

VarDecl* DeclContext::getDecl(std::string_view name) const {
    const auto it = _declMap.find(name);
    if (it == _declMap.end()) {
        return nullptr;
    }

    return it->second;
}

void DeclContext::addDecl(VarDecl* decl) {
    _declMap[decl->getName()] = decl;
}

DeclContext* DeclContext::create(CypherAST* ast, DeclContext* parent) {
    DeclContext* ctxt = new DeclContext(parent);
    ast->addDeclContext(ctxt);
    return ctxt;
}
