#include "VarDecl.h"

#include "ASTContext.h"
#include "DeclContext.h"

using namespace db;

VarDecl::VarDecl(const std::string& name)
    : _name(name)
{
}

VarDecl::~VarDecl() {
}

VarDecl* VarDecl::create(ASTContext* astCtxt,
                         DeclContext* declContext,
                         const std::string& name) {
    if (declContext->getDecl(name)) {
        return nullptr;
    }

    VarDecl* decl = new VarDecl(name);
    astCtxt->addVarDecl(decl);
    declContext->addDecl(decl);

    return decl;
}