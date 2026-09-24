#pragma once

#include <stddef.h>
#include <span>

namespace db {

class Expr;

class ExprDepth {
public:
    ExprDepth() = delete;
    ~ExprDepth() = delete;

    // The first expression nested deeper than @param maxDepth, where a leaf is one level
    // deep; nullptr when there is none
    static const Expr* findDeeperThan(std::span<Expr* const> expressions, size_t maxDepth);
};

}
