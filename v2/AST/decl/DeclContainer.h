#pragma once

#include <vector>

#include "decl/DeclID.h"
#include "decl/EvaluatedType.h"

namespace db {

class VarDecl;

class DeclContainer {
public:
    DeclContainer();
    ~DeclContainer();

    DeclContainer(const DeclContainer&) = delete;
    DeclContainer(DeclContainer&&) = delete;
    DeclContainer& operator=(const DeclContainer&) = delete;
    DeclContainer& operator=(DeclContainer&&) = delete;

    VarDecl& newDecl(EvaluatedType type);
    VarDecl& newDecl(EvaluatedType type, std::string_view name);

    VarDecl& getVariable(DeclID id) {
        return *_decls[id.value()];
    }

    const VarDecl& getDecl(DeclID id) const {
        return *_decls[id.value()];
    }

private:
    std::vector<std::unique_ptr<VarDecl>> _decls;
    DeclID _nextID {0};
};

}
