#pragma once

#include <vector>

#include "attribution/DeclID.h"
#include "attribution/EvaluatedType.h"

namespace db {

class VariableDecl;

class DeclContainer {
public:
    DeclContainer();
    ~DeclContainer();

    DeclContainer(const DeclContainer&) = delete;
    DeclContainer(DeclContainer&&) = delete;
    DeclContainer& operator=(const DeclContainer&) = delete;
    DeclContainer& operator=(DeclContainer&&) = delete;

    VariableDecl& newDecl(EvaluatedType type);
    VariableDecl& newDecl(EvaluatedType type, std::string_view name);

    VariableDecl& getVariable(DeclID id) {
        return *_decls[id.value()];
    }

    const VariableDecl& getDecl(DeclID id) const {
        return *_decls[id.value()];
    }

private:
    std::vector<std::unique_ptr<VariableDecl>> _decls;
    DeclID _nextID {0};
};

}
