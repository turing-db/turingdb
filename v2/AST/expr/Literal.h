#pragma once

#include <unordered_map>
#include <string_view>
#include <stdint.h>

#include "decl/EvaluatedType.h"

namespace db::v2 {

class CypherAST;
class Expr;
class Symbol;

class Literal {
public:
    friend CypherAST;

    enum class Kind {
        NULL_LITERAL,
        BOOL,
        INTEGER,
        DOUBLE,
        STRING,
        CHAR,
        MAP
    };

    virtual Kind getKind() const = 0;

    virtual EvaluatedType getType() const = 0;

protected:
    Literal() = default;
    virtual ~Literal() = default;
};

class NullLiteral : public Literal {
public:
    static NullLiteral* create(CypherAST* ast);

    constexpr Kind getKind() const override { return Kind::NULL_LITERAL; }

    constexpr EvaluatedType getType() const override { return EvaluatedType::Null; }

private:
    NullLiteral() = default;
    ~NullLiteral() = default;
};

class BoolLiteral : public Literal {
public:
    static BoolLiteral* create(CypherAST* ast, bool value);

    constexpr Kind getKind() const override { return Kind::BOOL; }

    constexpr EvaluatedType getType() const override { return EvaluatedType::Bool; }

    bool getValue() const { return _value; }

private:
    bool _value {false};

    BoolLiteral(bool value)
        : _value(value)
    {
    }

    ~BoolLiteral() = default;
};

class IntegerLiteral : public Literal {
public:
    static IntegerLiteral* create(CypherAST* ast, int64_t value);

    constexpr Kind getKind() const override { return Kind::INTEGER; }

    constexpr EvaluatedType getType() const override { return EvaluatedType::Integer; }

    int64_t getValue() const { return _value; }

private:
    int64_t _value {0};

    IntegerLiteral(int64_t value)
        : _value(value)
    {
    }

    ~IntegerLiteral() = default;
};

class DoubleLiteral : public Literal {
public:
    static DoubleLiteral* create(CypherAST* ast, double value);

    constexpr Kind getKind() const override { return Kind::DOUBLE; }

    constexpr EvaluatedType getType() const override { return EvaluatedType::Double; }

    double getValue() const { return _value; }

private:
    double _value {0.0};

    DoubleLiteral(double value)
        : _value(value)
    {
    }

    ~DoubleLiteral() = default;
};

class StringLiteral : public Literal {
public:
    static StringLiteral* create(CypherAST* ast, std::string_view value);

    constexpr Kind getKind() const override { return Kind::STRING; }

    constexpr EvaluatedType getType() const override { return EvaluatedType::String; }

    std::string_view getValue() const { return _value; }

private:
    std::string_view _value;

    StringLiteral(std::string_view value)
        : _value(value)
    {
    }

    ~StringLiteral() = default;
};

class CharLiteral : public Literal {
public:
    static CharLiteral* create(CypherAST* ast, char value);

    constexpr Kind getKind() const override { return Kind::CHAR; }

    constexpr EvaluatedType getType() const override { return EvaluatedType::Char; }

    char getValue() const { return _value; }

private:
    char _value {0};

    CharLiteral(char value)
        : _value(value)
    {
    }

    ~CharLiteral() = default;
};

class MapLiteral : public Literal {
public:
    using ExprMap = std::unordered_map<Symbol*, Expr*>;
    using ExprMapConstIterator = ExprMap::const_iterator;

    constexpr Kind getKind() const override { return Kind::MAP; }

    constexpr EvaluatedType getType() const override { return EvaluatedType::Map; }

    static MapLiteral* create(CypherAST* ast);

    void set(Symbol* key, Expr* value);

    ExprMapConstIterator begin() const { return _map.cbegin(); }
    ExprMapConstIterator end() const { return _map.cend(); }

private:
    ExprMap _map;

    MapLiteral();
    ~MapLiteral();
};

}
