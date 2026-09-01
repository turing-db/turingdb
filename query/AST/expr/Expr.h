#pragma once

#include <stdint.h>

#include "decl/EvaluatedType.h"
#include "decl/ListShape.h"

#include "EnumToString.h"

namespace db {

class CypherAST;
class VarDecl;

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
        INDEX,
        LIST,

        _SIZE
    };

    enum class Flags : uint8_t {
        NONE = 0,
        AGGREGATE = 1U << 0U,
        DYNAMIC = 1U << 1U,
    };

    Kind getKind() const { return _exprKind; }

    virtual EvaluatedType getType() const { return _type; }

    // How deeply a List-typed expression nests and what its innermost elements are.
    // What an UNWIND of this expression binds its variable to.
    const ListShape& getListShape() const { return _listShape; }

    const VarDecl* getExprVarDecl() const { return _exprVarDecl; }

    std::string_view getName() const {
        return _name;
    }

    void setType(EvaluatedType type) { _type = type; }

    void setListShape(const ListShape& shape) { _listShape = shape; }

    void setExprVarDecl(const VarDecl* decl) { _exprVarDecl = decl; }

    void setName(std::string_view name) {
        _name = name;
    }

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
    const VarDecl* _exprVarDecl {nullptr};
    Kind _exprKind {Kind::BINARY};
    EvaluatedType _type {EvaluatedType::Invalid};
    ListShape _listShape;
    Flags _flags {Flags::NONE};
    std::string_view _name;
};

using ExprKindDescription = EnumToString<Expr::Kind>::Create<
    EnumStringPair<Expr::Kind::BINARY, "BINARY">,
    EnumStringPair<Expr::Kind::UNARY, "UNARY">,
    EnumStringPair<Expr::Kind::STRING, "STRING">,
    EnumStringPair<Expr::Kind::ENTITY_TYPES, "ENTITY_TYPES">,
    EnumStringPair<Expr::Kind::PROPERTY, "PROPERTY">,
    EnumStringPair<Expr::Kind::PATH, "PATH">,
    EnumStringPair<Expr::Kind::SYMBOL, "SYMBOL">,
    EnumStringPair<Expr::Kind::LITERAL, "LITERAL">,
    EnumStringPair<Expr::Kind::FUNCTION_INVOCATION, "FUNCTION_INVOCATION">,
    EnumStringPair<Expr::Kind::INDEX, "INDEX">,
    EnumStringPair<Expr::Kind::LIST, "LIST">
>;

}
