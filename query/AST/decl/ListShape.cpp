#include "ListShape.h"

using namespace db;

ListShape::ListShape() {
}

ListShape::ListShape(EvaluatedType leafType, size_t depth)
    : _leafType(leafType),
    _depth(depth)
{
}

EvaluatedType ListShape::unwoundType() const {
    if (_depth > 1) {
        return EvaluatedType::List;
    }

    if (_depth == 1 && _leafType != EvaluatedType::Invalid) {
        return _leafType;
    }

    return EvaluatedType::ListItem;
}

ListShape ListShape::unwound() const {
    if (_depth <= 1) {
        return ListShape();
    }

    return ListShape(_leafType, _depth - 1);
}

ListShape ListShape::collecting(EvaluatedType type, const ListShape& shape) {
    if (type == EvaluatedType::List) {
        return ListShape(shape.getLeafType(), shape.getDepth() + 1);
    }

    // A tagged scalar names no type its list could be homogeneous in, so the list it
    // gathers into hands tagged scalars back out.
    if (type == EvaluatedType::ListItem) {
        return ListShape(EvaluatedType::Invalid, 1);
    }

    return ListShape(type, 1);
}
