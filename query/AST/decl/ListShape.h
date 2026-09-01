#pragma once

#include <stddef.h>

#include "decl/EvaluatedType.h"

namespace db {

/**
 * @brief How deeply a list-typed expression nests, and the one type its innermost
 * elements share.
 *
 * A list is homogeneous or it is nothing: either every element carries the same type -
 * at every level - or the list hands out tagged scalars and the leaf type is Invalid.
 * That makes a nested list type two numbers rather than a tree: `[[1, 2], [3]]` is depth
 * 2 over Integer, and it is the shape an UNWIND reads to know what it binds.
 */
class ListShape {
public:
    ListShape();
    ListShape(EvaluatedType leafType, size_t depth);

    EvaluatedType getLeafType() const { return _leafType; }
    size_t getDepth() const { return _depth; }

    bool isList() const { return _depth > 0; }

    // The type one UNWIND of this shape binds its variable to, and the shape that
    // variable then has: a list one level shallower, or the leaf type at the bottom.
    // A leaf no type names binds a tagged scalar, as does unwinding a non-list.
    EvaluatedType unwoundType() const;
    ListShape unwound() const;

    // The shape a collect of an expression of @param type and @param shape gathers into:
    // one level deeper than what it collects.
    static ListShape collecting(EvaluatedType type, const ListShape& shape);

private:
    EvaluatedType _leafType {EvaluatedType::Invalid};
    size_t _depth {0};
};

}
