#include "NLExecutor.h"

#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "CellOrder.h"
#include "metadata/PropertyType.h"

#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLSortOrder.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

// 3-way compare two rows of a non-null orderable column (an ID column, or a plain scalar
// a procedure yielded): negative if row a sorts before row b, positive if after, zero if
// they are equal.
template <typename ElementType>
int compareColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    const ElementType& valueA = raw[a];
    const ElementType& valueB = raw[b];

    if (sortsBefore(valueA, valueB)) {
        return -1;
    } else if (sortsBefore(valueB, valueA)) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a nullable value column. A null sorts after every
// value, so an ascending order places nulls last (matching Cypher's ORDER BY),
// and two nulls tie; non-null values compare by their natural order.
template <typename Primitive>
int compareOptColumn(const Column* column, size_t a, size_t b) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    const bool aNull = !typedColumn->isValid(a);
    const bool bNull = !typedColumn->isValid(b);
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    const Primitive& valueA = typedColumn->getValue(a);
    const Primitive& valueB = typedColumn->getValue(b);

    if (sortsBefore(valueA, valueB)) {
        return -1;
    } else if (sortsBefore(valueB, valueA)) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a type-erased column of tagged scalars. Cells need not share
// a type, so a pair of different types compares by the order those types sort in; nulls
// tie and sort after every value, as they do in a nullable value column.
int compareCellColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<CellView>*>(column)->getRaw();
    const std::strong_ordering order = raw[a] <=> raw[b];

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a list or a map column. Two lists order lexicographically
// on the element order above, and two maps by size, then keys, then values, so either can
// be the key the rows are sorted on.
template <typename View>
int compareContainerColumn(const Column* column, size_t a, size_t b) {
    const auto& raw = static_cast<const ColumnVector<View>*>(column)->getRaw();
    const std::strong_ordering order = raw[a] <=> raw[b];

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// 3-way compare two rows of a path column. The empty path a null is spelled as sorts after
// every path, as compareOptColumn places a null after every value.
int comparePathColumn(const Column* column, size_t a, size_t b) {
    const std::vector<GraphPath>& raw = static_cast<const ColumnVector<GraphPath>*>(column)->getRaw();
    const GraphPath& pathA = raw[a];
    const GraphPath& pathB = raw[b];

    const bool aNull = pathA.isNull();
    const bool bNull = pathB.isNull();
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    const std::strong_ordering order = PathView {pathA.getIDs()} <=> PathView {pathB.getIDs()};

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

// The nullable sibling, for a list or a map read out of a property: a row holding none
// sorts after every value, as compareOptColumn places a null after every value.
template <typename View>
int compareOptContainerColumn(const Column* column, size_t a, size_t b) {
    const ColumnOptVector<View>* typedColumn = static_cast<const ColumnOptVector<View>*>(column);

    const bool aNull = !typedColumn->isValid(a);
    const bool bNull = !typedColumn->isValid(b);
    if (aNull || bNull) {
        if (aNull && bNull) {
            return 0;
        }

        return aNull ? 1 : -1;
    }

    const std::strong_ordering order = typedColumn->getValue(a) <=> typedColumn->getValue(b);

    if (order == std::strong_ordering::less) {
        return -1;
    } else if (order == std::strong_ordering::greater) {
        return 1;
    }

    return 0;
}

}

NLCompareFunction NLExecutor::selectCellCompareFunction() {
    return &compareCellColumn;
}

NLCompareFunction NLExecutor::selectListCompareFunction() {
    return &compareContainerColumn<ListView>;
}

NLCompareFunction NLExecutor::selectOptListCompareFunction() {
    return &compareOptContainerColumn<ListView>;
}

NLCompareFunction NLExecutor::selectCompareFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return &compareColumn<NodeID>;
        break;

        case NLChunkKind::EdgeID:
            return &compareColumn<EdgeID>;
        break;

        case NLChunkKind::EdgeTypeID:
            return &compareColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelSetID:
            return &compareColumn<LabelSetID>;
        break;

        case NLChunkKind::LabelID:
            return &compareColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &compareColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &compareColumn<ValueType>;
        break;

        case NLChunkKind::UInt64:
            return &compareColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Int64:
            return &compareColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &compareColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &compareColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &compareColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            throw IRException("cannot sort by an embedding column");
        break;

        case NLChunkKind::List:
            return &compareContainerColumn<ListView>;
        break;

        case NLChunkKind::Map:
            return &compareContainerColumn<MapView>;
        break;

        case NLChunkKind::Path:
            return &comparePathColumn;
        break;

        case NLChunkKind::DateTime:
            return &compareColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &compareColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &compareColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::PathRef:
            throw IRException("A path column cannot be a sort key: expand it into its list first");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}

// A manual switch, not dispatchOptChunkKind, because an embedding has no order:
// dispatching would instantiate the comparator for std::span<const float>, which does not
// compile. The embedding case throws instead, so that instantiation is never named.
NLCompareFunction NLExecutor::selectOptCompareFunction(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::EdgeTypeID:
            return &compareOptColumn<EdgeTypeID>;
        break;

        case NLChunkKind::LabelID:
            return &compareOptColumn<LabelID>;
        break;

        case NLChunkKind::PropertyTypeID:
            return &compareOptColumn<PropertyTypeID>;
        break;

        case NLChunkKind::ValueTypeCode:
            return &compareOptColumn<ValueType>;
        break;

        case NLChunkKind::Int64:
            return &compareOptColumn<types::Int64::Primitive>;
        break;

        case NLChunkKind::UInt64:
            return &compareOptColumn<types::UInt64::Primitive>;
        break;

        case NLChunkKind::Double:
            return &compareOptColumn<types::Double::Primitive>;
        break;

        case NLChunkKind::Bool:
            return &compareOptColumn<types::Bool::Primitive>;
        break;

        case NLChunkKind::String:
            return &compareOptColumn<types::String::Primitive>;
        break;

        case NLChunkKind::Embedding:
            throw IRException("cannot sort by an embedding column");
        break;

        case NLChunkKind::List:
            return &compareOptContainerColumn<ListView>;
        break;

        case NLChunkKind::Map:
            return &compareOptContainerColumn<MapView>;
        break;

        case NLChunkKind::DateTime:
            return &compareOptColumn<types::DateTime::Primitive>;
        break;

        case NLChunkKind::Duration:
            return &compareOptColumn<types::Duration::Primitive>;
        break;

        case NLChunkKind::Point:
            return &compareOptColumn<types::Point::Primitive>;
        break;

        case NLChunkKind::NodeID:
        case NLChunkKind::EdgeID:
        case NLChunkKind::Path:
        case NLChunkKind::PathRef:
            throw IRException("A node, an edge or a path column holds its own null, so no nullable chunk wraps one");
        break;

        case NLChunkKind::LabelSetID:
            throw IRException("A label set column is read as rows, so no nullable chunk wraps one");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return nullptr;
}
