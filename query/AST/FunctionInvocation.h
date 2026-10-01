#pragma once

#include "FunctionSignature.h"

namespace db {

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

    void setSignature(const FunctionSignature* signature) { _signature = signature; }
    const FunctionSignature* getSignature() const { return _signature; }

    // Whether the invocation is over the distinct values of its argument -
    // count(DISTINCT x) rather than count(x)
    void setDistinct(bool distinct) { _distinct = distinct; }
    bool isDistinct() const { return _distinct; }

private:
    QualifiedName* _name {nullptr};
    ExprChain* _arguments {nullptr};
    const FunctionSignature* _signature {nullptr};
    bool _distinct {false};

    FunctionInvocation(QualifiedName* name)
        : _name(name)
    {
    }

    ~FunctionInvocation() = default;
};

}
