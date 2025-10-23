#pragma once

#include "decl/EvaluatedType.h"

#include <stdint.h>

namespace db::v2 {

class CypherAST;

class Expr {
public:
    friend CypherAST;

    enum class Kind : uint8_t {
        BINARY = 0,
        UNARY,
        STRING,
        ENTITY_TYPES,
        PROPERTY,
        PATH,
        SYMBOL,
        LITERAL,
        FUNCTION_INVOCATION,
    };

    enum class Flags : uint8_t {
        NONE = 0,
        AGGREGATE = 1 << 0,
        DYNAMIC = 1 << 1,
    };

    Kind getKind() const { return _exprKind; }

    virtual EvaluatedType getType() const { return _type; }

    void setType(EvaluatedType type) { _type = type; }

    [[nodiscard]] bool isAggregate() const {
        return ((uint8_t)_flags & (uint8_t)Flags::AGGREGATE) != 0;
    }

    [[nodiscard]] bool isDynamic() const {
        return ((uint8_t)_flags & (uint8_t)Flags::DYNAMIC) != 0;
    }

    void setAggregate() {
        _flags = (Flags)((uint8_t)_flags | (uint8_t)Flags::AGGREGATE);
    }

    void setDynamic() {
        _flags = (Flags)((uint8_t)_flags | (uint8_t)Flags::DYNAMIC);
    }

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
    EvaluatedType _type {EvaluatedType::Invalid};
    Flags _flags {Flags::NONE};
};

}
