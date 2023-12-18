#include "DeclContext.h"

#include "VarDecl.h"

using namespace db;

DeclContext::DeclContext()
{
}

DeclContext::~DeclContext() {
}

VarDecl* DeclContext::getDecl(const std::string& name) const {
    const auto it = _declMap.find(name);
    if (it == _declMap.end()) {
        return nullptr;
    }

    return it->second;
}

void DeclContext::addDecl(VarDecl* decl) {
    _decls.push_back(decl);
    _declMap[decl->getName()] = decl;
}
