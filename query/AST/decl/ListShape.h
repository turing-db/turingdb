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
 * 2 over Integer, and it is the shape an UNWIND reads to know what it binds. A leaf of
 * Null names no type at all: `[null]` and the empty list `[]` are depth 1 over Null.
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

    // The shape a value of @param type and @param shape gives a column of lists it fills. A
    // list whose shape no type names holds tagged cells, while a null holds no list at all.
    static ListShape selected(EvaluatedType type, const ListShape& shape);

    // The shape of one column holding lists of @param carried and of @param value: the one
    // list when the other is none, and the two joined otherwise
    static ListShape unified(const ListShape& carried, const ListShape& value);

    // The shape of a list holding the elements of lists of both shapes. A leaf of Null
    // constrains nothing, so it takes the other shape wherever that one nests as deep,
    // and lists nesting as deep over two leaves keep their depth over tagged scalars.
    static ListShape joined(const ListShape& left, const ListShape& right);

    // The shape of the one-element list a value makes: a null names no type of its own, and a
    // value no list is homogeneous in makes a list of tagged scalars
    static ListShape singleton(EvaluatedType type, const ListShape& shape);

private:
    EvaluatedType _leafType {EvaluatedType::Invalid};
    size_t _depth {0};
};

}
