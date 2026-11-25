#include "NLOps.h"

#include "NLDialect.h"

#include <optional>

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SmallVector.h"

#include "IRValueTypes.h"
#include "ColumnIndicesFormat.h"
#include "EdgeDirectionsFormat.h"
#include "PathStepsFormat.h"
#include "LabelAlternativesFormat.h"
#include "StorageAttributes.h"
#include "MergePatternShape.h"
#include "NullLiteralType.h"
#include "GroupAggregateKindsFormat.h"
#include "PropertyScanLiteral.h"
#include "BoundEntityIDs.h"
#include "BoundValueFormat.h"
#include "NLChunkTypes.h"

using namespace mlir;
using namespace mlir::nl;

namespace storage = mlir::storage;

#define GET_OP_CLASSES
#include "NLOps.cpp.inc"

namespace {

bool writesAMapPerRow(Type chunkType) {
    const auto chunk = dyn_cast<ChunkType>(chunkType);

    return holdsAMapPerRow(chunkType) || (chunk && isNullLiteralType(chunk.getElementType()));
}

}

LogicalResult EntityIDOf::verify() {
    const Type input = valueElementOf(getOperand().getType());
    if (!storage::isEntityIDOfInputType(input)) {
        return emitOpError("requires a value, a node or an edge, not a path, got ") << input;
    }

    const Type result = cast<ChunkType>(getResult().getType()).getElementType();
    if (!storage::isEntityIDType(result)) {
        return emitOpError("requires a node or an edge result, got ") << result;
    }

    return success();
}

LogicalResult CreateVectorIndex::verify() {
    if (getDimension() == 0) {
        return emitOpError("must hold vectors of at least one dimension");
    }

    return success();
}

// A cross product yields one chunk per crossed column - the outer columns
// followed by the inner - each keeping its input chunk's element type, since
// the broadcast only changes the row count, not the element kind.
LogicalResult CrossProduct::inferReturnTypes(MLIRContext* context,
                                             std::optional<Location> location,
                                             CrossProduct::Adaptor adaptor,
                                             SmallVectorImpl<Type>& inferredReturnTypes) {
    SmallVector<Type, 8> chunkTypes;

    for (const Type columnType : adaptor.getOuterColumns().getTypes()) {
        chunkTypes.push_back(columnType);
    }

    for (const Type columnType : adaptor.getInnerColumns().getTypes()) {
        chunkTypes.push_back(columnType);
    }

    inferredReturnTypes.push_back(IteratorType::get(context, chunkTypes));

    return success();
}

LogicalResult ExistsMark::verify() {
    if (getTag() && getRow()) {
        return emitOpError("marks either the rows a tag names or the one row `row` names, not both");
    }

    return success();
}

LogicalResult CountSubqueryTally::verify() {
    const bool hasTag = getTag() != nullptr;
    const bool hasRows = getRows() != nullptr;

    if (hasTag == hasRows) {
        return emitOpError("counts either the rows of a tag or a row count, exactly one of the two");
    } else if (getRow() && !hasRows) {
        return emitOpError("names the row a row count answers for, which a tagged tally has none of");
    }

    return success();
}

LogicalResult RowListsCollect::verify() {
    if (getTag() && getRow()) {
        return emitOpError("stages under either the rows a tag names or the one row `row` names, not both");
    }

    return verifyReadAsCells(getOperation(), getValue().getType());
}

// An nl.hash_join_collect must append at least one column: the build side's row
// count is read from the first buffer, and one of the columns is the key the
// buffer op names.
LogicalResult HashJoinCollect::verify() {
    if (getColumns().empty()) {
        return emitOpError("requires at least one column to collect");
    }

    return success();
}

// An nl.hash_join_probe reads at least one probe column - one of them is the key -
// and emits the probe columns unchanged followed by the build ones, so it has at
// least as many results as columns and the first of them keep the probe types.
LogicalResult HashJoinProbe::verify() {
    const OperandRange columns = getColumns();
    if (columns.empty()) {
        return emitOpError("requires at least one column to probe with");
    }

    const auto iteratorType = cast<IteratorType>(getResult().getType());
    const ArrayRef<Type> chunkTypes = iteratorType.getChunkTypes();
    if (chunkTypes.size() < columns.size()) {
        return emitOpError("expects a chunk per probe column and per build column, but has ")
               << chunkTypes.size() << " chunks for " << columns.size() << " probe columns";
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != chunkTypes[columnIndex]) {
            return emitOpError("chunk ") << columnIndex
                                         << " must have the same type as probe column "
                                         << columnIndex;
        }
    }

    return success();
}

LogicalResult Split::verify() {
    const bool readsAString = llvm::isa<storage::StringType, storage::AnyType>(valueElementOf(getString().getType()));
    const bool readsDelimiters = llvm::isa<storage::StringType, storage::ListType, storage::AnyType>(
                                     valueElementOf(getDelimiter().getType()));
    if (!readsAString || !readsDelimiters) {
        return emitOpError("operands must be a string chunk and a string or list chunk, or chunks of tagged cells");
    }

    const storage::ListType listType = llvm::dyn_cast<storage::ListType>(valueElementOf(getResult().getType()));
    if (!listType || !llvm::isa<storage::StringType>(listType.getElementType())) {
        return emitOpError("result must be a chunk of string lists");
    }

    return success();
}

LogicalResult CreateNode::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult CreateEdge::verify() {
    const StringAttr edgeType = dyn_cast<StringAttr>(getEdgeType());
    if (edgeType && edgeType.getValue().empty()) {
        return emitOpError("requires a non-empty edge type");
    }

    if (getPropNames().size() != getPropValues().size()) {
        return emitOpError("prop_names and prop_values must have the same count, but has ")
               << getPropNames().size() << " names and " << getPropValues().size() << " values";
    }

    return success();
}

LogicalResult Merge::verify() {
    const LogicalResult pattern = verifyMergePattern(getOperation(),
                                                     getNodeLabels(),
                                                     getNodePropNames(),
                                                     getEdgeTypes(),
                                                     getEdgePropNames(),
                                                     getEdgeDirectionsAttr(),
                                                     getPendingNodesAttr(),
                                                     getBoundNodes().size(),
                                                     getBoundPending().size(),
                                                     getNodePropValues().size(),
                                                     getEdgePropValues().size());
    if (failed(pattern)) {
        return pattern;
    }

    const size_t expectedResults = mergeResultCount(getNodeLabels(), getCarriedColumns().size());
    if (getResults().size() != expectedResults) {
        return emitOpError("must produce one chunk per chain entity, the created mask and one "
                           "per carried chunk - ")
               << expectedResults << " in all, but produces " << getResults().size();
    }

    return success();
}

LogicalResult SetNodeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetEdgeProperty::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    return success();
}

LogicalResult SetNodeProperties::verify() {
    if (!writesAMapPerRow(getValue().getType())) {
        return emitOpError("writes a chunk of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult SetEdgeProperties::verify() {
    if (!writesAMapPerRow(getValue().getType())) {
        return emitOpError("writes a chunk of maps or of tagged cells, optionally nullable, or the null literal");
    } else if (!::db::namesPropertyElementTypes(getCreatedTypesAttr())) {
        return emitOpError("creates each property with the element type of a stored value");
    }

    return success();
}

LogicalResult ImportGraph::verify() {
    if (!::db::namesImportedPropertyTypes(getFormat(), getTypesAttr())) {
        return emitOpError("types each property as a datetime, duration or point a jsonl import reads, or a duration a parquet import reads");
    }

    return success();
}
