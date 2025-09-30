#include "VarDecl.h"

#include "CypherAST.h"
#include "decl/DeclContext.h"

using namespace db::v2;

VarDecl* VarDecl::create(CypherAST* ast,
                         DeclContext* declContext,
                         std::string_view name,
                         EvaluatedType type) {
    VarDecl* decl = new VarDecl(type, name);
    ast->addVarDecl(decl);

    // If unnamed variable, no need to add it to the context
    if (!decl->getName().empty()) {
        declContext->addDecl(decl);
    }

    return decl;
}
