#pragma once

#include <variant>

namespace db {

class Expression;
class Statement;
class SubStatement;
class Pattern;
class PatternEntity;
class PatternPart;
class PatternNode;
class Projection;
class MapLiteral;
class SinglePartQuery;
class QueryCommand;

class ASTNode {
public:
    using Variant = std::variant<Expression*,
                                 Statement*,
                                 SubStatement*,
                                 Pattern*,
                                 PatternEntity*,
                                 PatternPart*,
                                 PatternNode*,
                                 Projection*,
                                 MapLiteral*,
                                 SinglePartQuery*,
                                 QueryCommand*>;

    ASTNode() = delete;
    ~ASTNode() = default;

    explicit ASTNode(const Variant& variant)
        : _variant(variant)
    {
    }

    ASTNode(const ASTNode&) = default;
    ASTNode(ASTNode&&) = default;
    ASTNode& operator=(const ASTNode&) = default;
    ASTNode& operator=(ASTNode&&) = default;

    template <typename T>
    T* as() {
        if (auto* ptr = std::get_if<T>(&_variant)) {
            return *ptr;
        }

        return nullptr;
    }

    template <typename T>
    const T* as() const {
        if (auto* ptr = std::get_if<T>(&_variant)) {
            return *ptr;
        }

        return nullptr;
    }

private:
    Variant _variant;
};

}
