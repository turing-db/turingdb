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
    } else if (_depth == 1 && _leafType != EvaluatedType::Invalid) {
        return _leafType;
    }

    return EvaluatedType::Any;
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
    } else if (type == EvaluatedType::Any) {
        // A tagged scalar names no type its list could be homogeneous in, so the list it
        // gathers into hands tagged scalars back out.
        return ListShape(EvaluatedType::Invalid, 1);
    }

    return ListShape(type, 1);
}

ListShape ListShape::selected(EvaluatedType type, const ListShape& shape) {
    const bool namesNoShape = type == EvaluatedType::List && !shape.isList();

    return namesNoShape ? ListShape(EvaluatedType::Invalid, 1) : shape;
}

ListShape ListShape::unified(const ListShape& carried, const ListShape& value) {
    if (!value.isList()) {
        return carried;
    } else if (!carried.isList()) {
        return value;
    }

    return joined(carried, value);
}

ListShape ListShape::joined(const ListShape& left, const ListShape& right) {
    const bool sameDepth = left._depth == right._depth;
    const bool leftNamesNoType = left._leafType == EvaluatedType::Null && left._depth <= right._depth;
    const bool rightNamesNoType = right._leafType == EvaluatedType::Null && right._depth <= left._depth;

    if (rightNamesNoType || (sameDepth && left._leafType == right._leafType)) {
        return left;
    } else if (leftNamesNoType) {
        return right;
    } else if (sameDepth) {
        return ListShape(EvaluatedType::Invalid, left._depth);
    }

    return ListShape(EvaluatedType::Invalid, 1);
}

ListShape ListShape::singleton(EvaluatedType type, const ListShape& shape) {
    if (type != EvaluatedType::Null && !namesAListElementType(type)) {
        return ListShape(EvaluatedType::Invalid, 1);
    }

    return collecting(type, shape);
}
