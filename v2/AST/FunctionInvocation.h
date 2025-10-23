#pragma once

#include "FunctionSignature.h"

namespace db::v2 {

class CypherAST;
class QualifiedName;
class ExprChain;

class FunctionInvocation {
public:
    friend CypherAST;

    static FunctionInvocation* create(CypherAST* ast, QualifiedName* name);

    void setArguments(ExprChain* arguments) { _arguments = arguments; }

    QualifiedName* getName() const { return _name; }
    ExprChain* getArguments() const { return _arguments; }

    void setSignature(FunctionSignature* signature) { _signature = signature; }
    FunctionSignature* getSignature() const { return _signature; }

private:
    QualifiedName* _name {nullptr};
    ExprChain* _arguments {nullptr};
    FunctionSignature* _signature {nullptr};

    FunctionInvocation(QualifiedName* name)
        : _name(name)
    {
    }

    ~FunctionInvocation() = default;
};

}
