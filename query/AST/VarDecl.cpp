#include "VarDecl.h"

#include "ASTContext.h"
#include "DeclContext.h"

using namespace db;

VarDecl::VarDecl(DeclKind kind, const std::string& name)
    : _kind(kind),
    _name(name)
{
}

VarDecl::~VarDecl() {
}

VarDecl* VarDecl::create(ASTContext* astCtxt,
                         DeclContext* declContext,
                         const std::string& name,
                         DeclKind kind) {
    if (declContext->getDecl(name)) {
        return nullptr;
    }

    VarDecl* decl = new VarDecl(kind, name);
    astCtxt->addVarDecl(decl);
    declContext->addDecl(decl);

    return decl;
}
