#pragma once

#include <string_view>

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

private:
    QualifiedName* _name {nullptr};
    ExprChain* _arguments {nullptr};

    FunctionInvocation(QualifiedName* name)
        : _name(name)
    {
    }

    ~FunctionInvocation() = default;
};

}
