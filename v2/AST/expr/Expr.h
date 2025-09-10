#pragma once

#include "decl/EvaluatedType.h"

namespace db::v2 {

class CypherAST;

class Expr {
public:
    friend CypherAST;

    enum class Kind {
        BINARY = 0,
        UNARY,
        STRING,
        NODE_LABEL,
        PROPERTY,
        PATH,
        SYMBOL,
        LITERAL,
        PARAMETER,
    };

    Kind getKind() const { return _exprKind; }

    EvaluatedType getValueType() const { return _valueType; }

    void setValueType(EvaluatedType valueType) { _valueType = valueType; }

protected:
    explicit Expr(Kind kind)
        : _exprKind(kind)
    {
    }

    Expr(const Expr&) = delete;
    Expr(Expr&&) = delete;
    Expr& operator=(const Expr&) = delete;
    Expr& operator=(Expr&&) = delete;

    virtual ~Expr() = default;

private:
    Kind _exprKind;
    EvaluatedType _valueType {EvaluatedType::Invalid};
};

}
