#include "DBOps.h"
#include "DBDialect.h"

#include <optional>

#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRLiteralList.h"
#include "StorageEnums.h"
#include "ColumnIndicesFormat.h"
#include "EdgeDirectionsFormat.h"
#include "MergePatternShape.h"
#include "GroupAggregateKindsFormat.h"
#include "PropertyScanLiteral.h"

using namespace mlir;
using namespace mlir::db;

namespace storage = mlir::storage;

#define GET_OP_CLASSES
#include "DBOps.cpp.inc"

namespace {

LogicalResult verifyEdgeTypesNotEmpty(Operation* operation, ArrayAttr edgeTypes) {
    if (edgeTypes.empty()) {
        return operation->emitOpError("requires at least one edge type");
    }

    return success();
}

// The keyword that introduces each factor region in the textual form
// `db.cross_product factor { ... } factor { ... }`.
const char* const factorKeyword = "factor";

// The keyword db.hash_join spells its two key column indices after.
const char* const keysKeyword = "on";

// The db.output a union branch ends with, or a null Output when the branch is empty or
// ends on something else. A branch's output is what fills the union's result table.
Output getBranchOutput(Region& branch) {
    Operation* last = nullptr;
    if (!branch.empty()) {
        Block& block = branch.front();
        if (!block.empty()) {
            last = &block.back();
        }
    }

    return dyn_cast_or_null<Output>(last);
}

// The db.yield that terminates a factor region, or a null Yield if the region
// is empty or does not end with one. A factor's yield names the columns that
// factor contributes to the product.
Yield getFactorYield(Region& factor) {
    Operation* terminator = nullptr;
    if (!factor.empty()) {
        Block& block = factor.front();
        if (!block.empty()) {
            terminator = &block.back();
        }
    }

    return dyn_cast_or_null<Yield>(terminator);
}

// The branches of a union with results feed its results rather than the result table,
// so each one ends in a db.yield naming one column per result
LogicalResult verifyYieldingBranches(Union unionOp) {
    for (Region& branch : unionOp.getBranches()) {
        Yield yield = getFactorYield(branch);
        if (!yield) {
            return unionOp.emitOpError("each branch of a union with results must end with a db.yield");
        }

        if (yield.getColumns().size() != unionOp.getNumResults()) {
            return unionOp.emitOpError("each branch must yield one column per result");
        }
    }

    return success();
}

// Appends the columns yielded by a factor region to resultTypes, failing with a
// diagnostic if the factor is not terminated by a db.yield.
ParseResult appendFactorYieldTypes(OpAsmParser& parser,
                                   Region& factor,
                                   llvm::SmallVectorImpl<Type>& resultTypes) {
    Yield yield = getFactorYield(factor);
    if (!yield) {
        return parser.emitError(parser.getCurrentLocation(),
                                "factor must end with a db.yield");
    }

    for (const Type columnType : yield.getColumns().getTypes()) {
        resultTypes.push_back(columnType);
    }

    return success();
}

// Parses `factor { ... }` into a fresh region of result. The factor takes no
// operands and no block arguments, so the region is parsed with an empty
// argument list.
ParseResult parseFactorRegion(OpAsmParser& parser, OperationState& result) {
    if (parser.parseKeyword(factorKeyword)) {
        return failure();
    }

    Region* factor = result.addRegion();
    return parser.parseRegion(*factor, {});
}

// The results of a two-factor op - db.cross_product, db.hash_join - must be exactly the
// columns the two factors yield: the left factor's yielded columns followed by the right
// factor's. The yields drive the result types during parsing, so this guards the
// programmatic builder path and re-checks parsed IR. Shared by both verifiers.
LogicalResult verifyFactorResults(Operation* op, Region& leftFactor, Region& rightFactor) {
    Yield leftYield = getFactorYield(leftFactor);
    Yield rightYield = getFactorYield(rightFactor);
    if (!leftYield || !rightYield) {
        return op->emitOpError("each factor region must end with a db.yield");
    }

    // Each factor must contribute at least one column. A side's row count is read
    // from its first yielded column during lowering, so a factor that surfaces no
    // column (an empty db.yield) cannot be sized - reject it here at the db level.
    if (leftYield.getColumns().empty() || rightYield.getColumns().empty()) {
        return op->emitOpError("each factor must yield at least one column");
    }

    llvm::SmallVector<Type> expectedResultTypes;
    for (const Type columnType : leftYield.getColumns().getTypes()) {
        expectedResultTypes.push_back(columnType);
    }
    for (const Type columnType : rightYield.getColumns().getTypes()) {
        expectedResultTypes.push_back(columnType);
    }

    const Operation::result_type_range resultTypes = op->getResultTypes();
    if (resultTypes.size() != expectedResultTypes.size()) {
        return op->emitOpError("expects ") << expectedResultTypes.size()
                                           << " results, the columns yielded by the two factors, but has "
                                           << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < expectedResultTypes.size(); resultIndex++) {
        if (resultTypes[resultIndex] != expectedResultTypes[resultIndex]) {
            return op->emitOpError("result ") << resultIndex << " must be the yielded column type "
                                              << expectedResultTypes[resultIndex];
        }
    }

    return success();
}

// A literal list typed as homogeneous - db.unwind_const's typed column, db.const_list's
// typed list - must carry at least one element and every element must carry one shared
// type, or be a null, which rides a unit attr and shares whatever type the others carry.
// The elements are checked against each other and not against the spelled element type: a
// hand-written "s" parses as an untyped StringAttr, so comparing it to a !storage.string
// would reject valid IR. A homogeneous result paired with literals of another type is
// therefore not an op-level error; the runtime fill catches that on the element's type tag.
LogicalResult verifyHomogeneousElements(Operation* op, ArrayAttr elements) {
    if (elements.empty()) {
        return op->emitOpError("a homogeneous literal list must carry at least one element; "
                               "an empty list is the list_element form");
    }

    mlir::Type payloadType;
    for (const Attribute element : elements) {
        if (llvm::isa<UnitAttr>(element)) {
            continue;
        }

        const mlir::Type elementType = ::db::literalElementType(element);
        if (!elementType) {
            return op->emitOpError("literal list element is not a typed attribute");
        }

        if (!payloadType) {
            payloadType = elementType;
        } else if (elementType != payloadType) {
            return op->emitOpError("a homogeneous literal list requires every element to share one type");
        }
    }

    if (!payloadType) {
        return op->emitOpError("a homogeneous literal list must carry an element naming its type; "
                               "a list of nulls alone is the list_element form");
    }

    return success();
}

// db.limit and db.skip both pass their columns straight through, so the results
// must be exactly the input columns - same count, same types and in the same
// order. Their row count is read from the first column during lowering, so a
// pass-through over no column cannot be sized and is rejected here at the db
// level. Shared by both verifiers.
LogicalResult verifyPassThrough(Operation* op,
                                OperandRange columns,
                                Operation::result_range results) {
    if (columns.empty()) {
        return op->emitOpError("requires at least one column");
    }

    if (columns.size() != results.size()) {
        return op->emitOpError("expects ") << columns.size()
                                           << " results, one per input column, but has "
                                           << results.size();
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != results[columnIndex].getType()) {
            return op->emitOpError("result ") << columnIndex
                                              << " must have the same type as input column "
                                              << columnIndex;
        }
    }

    return success();
}

// The hop region of an explore_paths: one block over (source, edge, end) yielding a boolean
// column, reading nothing defined outside it but constants
LogicalResult verifyHopRegion(Operation* op, Region& hop) {
    Block& block = hop.front();
    MLIRContext* context = op->getContext();

    const Type nodeColumn = ColumnType::get(context, storage::NodeIDType::get(context));
    const Type edgeColumn = ColumnType::get(context, storage::EdgeIDType::get(context));
    const llvm::SmallVector<Type, 3> expectedArguments {nodeColumn, edgeColumn, nodeColumn};

    if (block.getNumArguments() != expectedArguments.size()) {
        return op->emitOpError("hop region must take the source node, edge and end node columns");
    }

    for (size_t argumentIndex = 0; argumentIndex < expectedArguments.size(); argumentIndex++) {
        if (block.getArgument(static_cast<unsigned>(argumentIndex)).getType() != expectedArguments[argumentIndex]) {
            return op->emitOpError("hop region argument ") << argumentIndex << " must be "
                                                           << expectedArguments[argumentIndex];
        }
    }

    Yield yield = getFactorYield(hop);
    if (!yield) {
        return op->emitOpError("hop region must end with a db.yield");
    }

    const Type boolColumn = ColumnType::get(context, storage::BoolType::get(context));
    const bool yieldsOneMask = yield.getColumns().size() == 1 && yield.getColumns().front().getType() == boolColumn;
    if (!yieldsOneMask) {
        return op->emitOpError("hop region must yield exactly one ") << boolColumn;
    }

    for (Operation& inner : block) {
        for (const Value operand : inner.getOperands()) {
            if (const auto argument = dyn_cast<BlockArgument>(operand)) {
                if (argument.getOwner() != &block) {
                    return op->emitOpError("hop region reads a block argument of another region");
                }

                continue;
            }

            Operation* const definingOp = operand.getDefiningOp();
            const bool definedInside = definingOp->getBlock() == &block;
            if (!definedInside && !isa<ConstantOp>(definingOp)) {
                return op->emitOpError("hop region may only read constants from outside itself");
            }
        }
    }

    return success();
}

}

// Ensures each variable has a numeric name
void ScanEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanEdgesByType::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanOutEdgesByLabelSrc::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanInEdgesByLabelTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanOutEdgesByLabelTgt::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ScanInEdgesByLabelSrc::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

// Ensures each variable has a numeric name
void GetOutEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void ExplorePaths::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

LogicalResult ExplorePaths::verify() {
    const OperandRange carried = getColumnsToFilter();
    const ResultRange filtered = getFilteredColumns();
    if (carried.size() != filtered.size()) {
        return emitOpError("expects one filtered column per carried column, but carries ")
               << carried.size() << " and filters " << filtered.size();
    }

    for (size_t columnIndex = 0; columnIndex < carried.size(); columnIndex++) {
        if (carried[columnIndex].getType() != filtered[columnIndex].getType()) {
            return emitOpError("filtered column ") << columnIndex
                                                   << " must have the type of carried column "
                                                   << columnIndex;
        }
    }

    const std::optional<uint64_t> maxHops = getMaxHops();
    if (maxHops && *maxHops < getMinHops()) {
        return emitOpError("max_hops must be at least min_hops");
    }

    const std::optional<llvm::StringRef> edgeType = getEdgeType();
    if (edgeType && edgeType->empty()) {
        return emitOpError("edge_type must name an edge type");
    }

    if (const std::optional<ArrayAttr> endLabels = getEndLabels()) {
        if (endLabels->empty()) {
            return emitOpError("end_labels must name at least one label");
        }

        for (const Attribute label : *endLabels) {
            if (cast<StringAttr>(label).getValue().empty()) {
                return emitOpError("end_labels must name labels");
            }
        }
    }

    if (const std::optional<uint64_t> endColumn = getEndColumn()) {
        if (*endColumn >= carried.size()) {
            return emitOpError("end_column ") << *endColumn << " is not a carried column";
        }

        const auto endType = cast<ColumnType>(carried[*endColumn].getType());
        if (!isa<storage::NodeIDType>(endType.getType())) {
            return emitOpError("end_column must name a node column");
        }
    }

    if (getEndsOnSeed() && getEndColumn()) {
        return emitOpError("ends_on_seed names the end already named by end_column");
    }

    if (getDistinct()) {
        if (getMinHops() > 1) {
            return emitOpError("distinct is exact for a min_hops of at most one");
        }

        const bool undirected = getDirection() == storage::PathDirection::Both;
        if (undirected && getMinHops() != 0) {
            return emitOpError("distinct over both directions is exact for a min_hops of zero alone");
        }

        if (!getPaths().use_empty()) {
            return emitOpError("distinct emits no path, so paths must have no use");
        }
    }

    Region& hop = getHop();
    if (hop.empty()) {
        return success();
    }

    return verifyHopRegion(getOperation(), hop);
}

LogicalResult ExpandPath::verify() {
    const auto column = dyn_cast<ColumnType>(getResult().getType());
    const auto list = column ? dyn_cast<storage::ListType>(column.getType()) : storage::ListType();
    if (!list) {
        return emitOpError("must produce a list column");
    }

    const Type elementType = list.getElementType();
    if (getKind() == storage::PathExpansionKind::Edges) {
        if (!isa<storage::EdgeIDType>(elementType)) {
            return emitOpError("kind edges expands to a list of edge IDs");
        }
    } else if (!isa<storage::NodeIDType>(elementType)) {
        return emitOpError("kind sources and ends expand to a list of node IDs");
    }

    if (getKind() == storage::PathExpansionKind::Sources && !getSrcids()) {
        return emitOpError("kind sources reads the seed of each path from srcids");
    }

    return success();
}

LogicalResult MakePath::verify() {
    const OperandRange entities = getEntities();
    if (entities.empty()) {
        return emitOpError("a path runs through at least one node");
    }

    const auto elementOf = [](Value value) -> Type {
        const auto column = dyn_cast<ColumnType>(value.getType());
        return column ? column.getType() : Type();
    };

    if (!isa_and_nonnull<storage::NodeIDType>(elementOf(entities.front()))) {
        return emitOpError("a path opens on a node column");
    }

    size_t entityIndex = 1;
    while (entityIndex < entities.size()) {
        const Type element = elementOf(entities[entityIndex]);

        if (isa_and_nonnull<storage::PathRefType>(element)) {
            entityIndex++;
            continue;
        }

        if (!isa_and_nonnull<storage::EdgeIDType>(element)) {
            return emitOpError("a hop of a path is an edge column or a path column, at operand ") << entityIndex;
        }

        const bool landsOnANode = entityIndex + 1 < entities.size()
                               && isa_and_nonnull<storage::NodeIDType>(elementOf(entities[entityIndex + 1]));
        if (!landsOnANode) {
            return emitOpError("the edge column at operand ") << entityIndex << " lands on no node column";
        }

        entityIndex += 2;
    }

    return success();
}

void GetInEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetEdges::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetOutEdgesByType::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetInEdgesByType::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetOutEdgesByLabel::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

void GetInEdgesByLabel::getAsmResultNames(OpAsmSetValueNameFn setNameFn) {
    for (Value result : getResults()) {
        setNameFn(result, "");
    }
}

// Builds the op from just the result types - the left factor's yielded columns
// followed by the right factor's - and creates the two empty factor blocks. The
// caller fills each region and terminates it with a db.yield whose operands
// match the result types contributed by that factor. The insertion guard keeps
// the block creation from leaking out of the builder.
void CrossProduct::build(OpBuilder& builder, OperationState& state, TypeRange resultTypes) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    Region* leftFactor = state.addRegion();
    builder.createBlock(leftFactor);

    Region* rightFactor = state.addRegion();
    builder.createBlock(rightFactor);
}

// Custom syntax, mirroring the disconnected pattern it models:
//
//   %a, %b = db.cross_product factor { ... db.yield %x : ... }
//                             factor { ... db.yield %y : ... }
//
// Nothing is spelled after the regions: the result types are recovered from the
// two factors' yields - the left factor's yielded columns followed by the
// right factor's - matching the result count parsed from the `%a, %b =` list.
ParseResult CrossProduct::parse(OpAsmParser& parser, OperationState& result) {
    const bool regionsFailed = parseFactorRegion(parser, result)
                               || parseFactorRegion(parser, result)
                               || parser.parseOptionalAttrDict(result.attributes);
    if (regionsFailed) {
        return failure();
    }

    Region& leftFactor = *result.regions[0];
    Region& rightFactor = *result.regions[1];
    if (appendFactorYieldTypes(parser, leftFactor, result.types)
        || appendFactorYieldTypes(parser, rightFactor, result.types)) {
        return failure();
    }

    return success();
}

void CrossProduct::print(OpAsmPrinter& printer) {
    printer << " " << factorKeyword << " ";
    printer.printRegion(getLeftFactor());

    printer << " " << factorKeyword << " ";
    printer.printRegion(getRightFactor());

    printer.printOptionalAttrDict((*this)->getAttrs());
}

LogicalResult CrossProduct::verify() {
    return verifyFactorResults(getOperation(), getLeftFactor(), getRightFactor());
}

// Builds the op from the branch count alone and creates that many empty blocks. The
// caller fills each region with a query body and ends it with a db.output. The insertion
// guard keeps the block creation from leaking out of the builder, as CrossProduct's does.
void Union::build(OpBuilder& builder, OperationState& state, size_t branchCount) {
    build(builder, state, TypeRange {}, branchCount);
}

void Union::build(OpBuilder& builder, OperationState& state, TypeRange resultTypes, size_t branchCount) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    for (size_t branchIndex = 0; branchIndex < branchCount; branchIndex++) {
        Region* branch = state.addRegion();
        builder.createBlock(branch);
    }
}

// Custom syntax, one region per branch separated by commas:
//
//   db.union { ... db.output(%a) names ["name"] : ... },
//             { ... db.output(%b) names ["name"] : ... }
//
// A union with results spells their types after the regions:
//
//   %z = db.union { ... db.yield %a : ... }, { ... db.yield %b : ... } : !db.column<none>
ParseResult Union::parse(OpAsmParser& parser, OperationState& result) {
    do {
        Region* branch = result.addRegion();
        if (parser.parseRegion(*branch, {})) {
            return failure();
        }
    } while (succeeded(parser.parseOptionalComma()));

    if (succeeded(parser.parseOptionalColon())) {
        SmallVector<Type> resultTypes;
        if (parser.parseTypeList(resultTypes)) {
            return failure();
        }

        result.addTypes(resultTypes);
    }

    return parser.parseOptionalAttrDict(result.attributes);
}

void Union::print(OpAsmPrinter& printer) {
    llvm::interleave(getBranches(),
                     printer,
                     [&](Region& branch) {
                         printer << " ";
                         printer.printRegion(branch);
                     },
                     ",");

    const ResultRange results = getResults();
    if (!results.empty()) {
        printer << " : ";
        llvm::interleaveComma(results.getTypes(), printer);
    }

    printer.printOptionalAttrDict((*this)->getAttrs());
}

// A union emits one result table, so every branch has to end in a db.output and all of
// them have to name the same columns. The names are the query's, checked by the analyzer;
// what is re-checked here is the shape a hand-written module can still get wrong.
LogicalResult Union::verify() {
    const MutableArrayRef<Region> branches = getBranches();
    if (branches.size() < 2) {
        return emitOpError("requires at least two branches");
    }

    if (getNumResults() > 0) {
        return verifyYieldingBranches(*this);
    }

    Output first;
    for (Region& branch : branches) {
        Output output = getBranchOutput(branch);
        if (!output) {
            return emitOpError("each branch must end with a db.output");
        }

        if (!first) {
            first = output;
            continue;
        }

        if (output.getColumns().size() != first.getColumns().size()) {
            return emitOpError("every branch must output the same number of columns");
        }

        if (output.getColumnNamesAttr() != first.getColumnNamesAttr()) {
            return emitOpError("every branch must output the same column names");
        }
    }

    return success();
}

// Builds the op from the result types - the left factor's yielded columns followed
// by the right factor's - and the two key column indices, and creates the two empty
// factor blocks. The cross product's sibling builder, with the join keys added.
void HashJoin::build(OpBuilder& builder,
                     OperationState& state,
                     TypeRange resultTypes,
                     uint64_t leftKey,
                     uint64_t rightKey) {
    const OpBuilder::InsertionGuard guard(builder);

    state.addTypes(resultTypes);

    const Type keyIndexType = builder.getIntegerType(64, /*isSigned=*/false);
    state.addAttribute(getLeftKeyAttrName(state.name), builder.getIntegerAttr(keyIndexType, leftKey));
    state.addAttribute(getRightKeyAttrName(state.name), builder.getIntegerAttr(keyIndexType, rightKey));

    Region* leftFactor = state.addRegion();
    builder.createBlock(leftFactor);

    Region* rightFactor = state.addRegion();
    builder.createBlock(rightFactor);
}

// Custom syntax, the cross product's with the join keys spelled after the regions:
//
//   %a, %ka, %b, %kb = db.hash_join factor { ... db.yield %x, %k : ... }
//                                   factor { ... db.yield %y, %k : ... } on 1, 1
//
// The result types are recovered from the two factors' yields, exactly as
// db.cross_product recovers them.
ParseResult HashJoin::parse(OpAsmParser& parser, OperationState& result) {
    if (parseFactorRegion(parser, result) || parseFactorRegion(parser, result)) {
        return failure();
    }

    uint64_t leftKey = 0;
    uint64_t rightKey = 0;
    const bool keysFailed = parser.parseKeyword(keysKeyword)
                            || parser.parseInteger(leftKey)
                            || parser.parseComma()
                            || parser.parseInteger(rightKey);
    if (keysFailed) {
        return failure();
    }

    if (parser.parseOptionalAttrDict(result.attributes)) {
        return failure();
    }

    const Type keyIndexType = parser.getBuilder().getIntegerType(64, /*isSigned=*/false);
    result.addAttribute(getLeftKeyAttrName(result.name), IntegerAttr::get(keyIndexType, leftKey));
    result.addAttribute(getRightKeyAttrName(result.name), IntegerAttr::get(keyIndexType, rightKey));

    Region& leftFactor = *result.regions[0];
    Region& rightFactor = *result.regions[1];
    if (appendFactorYieldTypes(parser, leftFactor, result.types)
        || appendFactorYieldTypes(parser, rightFactor, result.types)) {
        return failure();
    }

    return success();
}

void HashJoin::print(OpAsmPrinter& printer) {
    printer << " " << factorKeyword << " ";
    printer.printRegion(getLeftFactor());

    printer << " " << factorKeyword << " ";
    printer.printRegion(getRightFactor());

    printer << " " << keysKeyword << " " << getLeftKey() << ", " << getRightKey();

    printer.printOptionalAttrDict((*this)->getAttrs(), {getLeftKeyAttrName(), getRightKeyAttrName()});
}

// The results line up with the two factors' yields exactly as a cross product's do, and
// each key index has to name a column of its own factor's yield - it is what the join
// matches on, so a key past the end names no column to read.
LogicalResult HashJoin::verify() {
    if (failed(verifyFactorResults(getOperation(), getLeftFactor(), getRightFactor()))) {
        return failure();
    }

    const size_t leftCount = getFactorYield(getLeftFactor()).getColumns().size();
    const size_t rightCount = getFactorYield(getRightFactor()).getColumns().size();

    if (getLeftKey() >= leftCount) {
        return emitOpError("left key column ") << getLeftKey() << " is past the "
                                               << leftCount << " columns the left factor yields";
    }

    if (getRightKey() >= rightCount) {
        return emitOpError("right key column ") << getRightKey() << " is past the "
                                                << rightCount << " columns the right factor yields";
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
    if (getEdgeType().empty()) {
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
        return emitOpError("must produce one column per chain entity, the created mask and one "
                           "per carried column - ")
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

// A label scan must name at least one label to filter by; a label-free scan of
// every node is db.scan_nodes, so an empty label list is malformed IR here.
LogicalResult ScanNodesByLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

// The edge sibling of ScanNodesByLabel::verify: a label-free scan of every edge is
// db.scan_edges, so an empty label list is malformed IR here too.
LogicalResult ScanOutEdgesByLabelSrc::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanInEdgesByLabelTgt::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanOutEdgesByLabelTgt::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult ScanInEdgesByLabelSrc::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult GetOutEdgesByLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult GetInEdgesByLabel::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult CountScanRows::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one scan to count");
    }

    const std::optional<llvm::StringRef> property = getProperty();
    if (property && property->empty()) {
        return emitOpError("requires a non-empty property name");
    }

    if (!property && getPropertyScan()) {
        return emitOpError("names the scan of a property it does not read");
    }

    if (getPropertyScan().value_or(0) >= getLabels().size()) {
        return emitOpError("reads a property from a scan it does not list");
    }

    return success();
}

LogicalResult ScanNodesByPropertyValue::verify() {
    if (getProperty().empty()) {
        return emitOpError("requires a non-empty property name");
    }

    const TypedAttr literal = getValue();
    if (!storage::isPropertyScanLiteral(literal)) {
        return emitOpError(storage::propertyScanLiteralKinds()) << literal.getType();
    }

    const std::optional<ArrayAttr> labels = getLabels();
    if (labels && labels->empty()) {
        return emitOpError("requires at least one label when labels are given");
    }

    return success();
}

LogicalResult CheckLabelConstraint::verify() {
    if (getLabels().empty()) {
        return emitOpError("requires at least one label");
    }

    return success();
}

LogicalResult CheckEdgeTypeConstraint::verify() {
    return verifyEdgeTypesNotEmpty(getOperation(), getEdgeTypes());
}

void Output::build(OpBuilder& builder, OperationState& state, ValueRange columns) {
    Output::build(builder, state, columns, ArrayAttr());
}

LogicalResult Output::verify() {
    const ArrayAttr columnNames = getColumnNamesAttr();
    if (columnNames && columnNames.size() != getColumns().size()) {
        return emitOpError("names must give one name per output column, but has ")
               << columnNames.size() << " names for " << getColumns().size() << " columns";
    }

    return success();
}

// db.case pairs each condition with the value the row takes when it holds, so the two
// operand groups must be parallel; a CASE with no branch is not a selection at all.
LogicalResult Case::verify() {
    const OperandRange conditions = getConditions();
    const OperandRange values = getValues();

    if (conditions.empty()) {
        return emitOpError("requires at least one branch");
    } else if (conditions.size() != values.size()) {
        return emitOpError("expects one value per condition, but has ")
               << values.size() << " values for " << conditions.size() << " conditions";
    }

    return success();
}

// db.limit passes its columns straight through, so the results must be exactly
// the input columns - same count, same types and in the same order.
LogicalResult Limit::verify() {
    return verifyPassThrough(getOperation(), getColumns(), getResults());
}

// db.skip passes its columns straight through, the same as db.limit.
LogicalResult Skip::verify() {
    return verifyPassThrough(getOperation(), getColumns(), getResults());
}

// db.sort passes its columns straight through reordered, so the results must be
// exactly the input columns - same count, same types, same order. The two key
// arrays must be parallel and every key must index a column that exists.
LogicalResult Sort::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();

    // A sort with no column has nothing to reorder, and its row count would have
    // no column to be read from during lowering. Reject it here at the db level.
    if (columns.empty()) {
        return emitOpError("requires at least one column");
    }

    if (columns.size() != results.size()) {
        return emitOpError("expects ") << columns.size()
                                       << " results, one per input column, but has "
                                       << results.size();
    }

    for (size_t columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
        if (columns[columnIndex].getType() != results[columnIndex].getType()) {
            return emitOpError("result ") << columnIndex
                                          << " must have the same type as input column "
                                          << columnIndex;
        }
    }

    const ArrayRef<int64_t> keyColumns = getKeyColumns();
    const ArrayRef<bool> keyAscending = getKeyAscending();

    // At least one key, and one direction per key: the two arrays describe the
    // same list of sort keys, so a mismatch is malformed IR.
    if (keyColumns.empty()) {
        return emitOpError("requires at least one sort key");
    }

    if (keyColumns.size() != keyAscending.size()) {
        return emitOpError("expects one ascending flag per key, but has ")
               << keyColumns.size() << " keys and " << keyAscending.size() << " flags";
    }

    // Every key names a column to sort by, so it must index one of the columns.
    for (const int64_t keyColumn : keyColumns) {
        const bool inRange = keyColumn >= 0 && static_cast<size_t>(keyColumn) < columns.size();
        if (!inRange) {
            return emitOpError("sort key ") << keyColumn << " is out of range for "
                                            << columns.size() << " columns";
        }
    }

    return success();
}

// db.remove_duplicates passes its columns straight through (minus duplicate rows),
// so the results must be exactly the input columns - same count, same types, same
// order - the shared pass-through check db.limit and db.skip use. The dedup key is
// the whole row, so - unlike db.sort - there are no key arrays to validate.
LogicalResult RemoveDuplicates::verify() {
    return verifyPassThrough(getOperation(), getColumns(), getResults());
}

// A call produces one column per yielded return value then one per carried column, so
// `yields`, the carry set and the results must line up. A call yielding nothing has no
// column to produce and no row count to be read from, so it is rejected here at the db
// level; the procedure's return values themselves are checked against the registry
// during lowering, which is where the registry is available.
LogicalResult CallProcedure::verify() {
    const ArrayAttr yields = getYields();
    const OperandRange carriedColumns = getCarriedColumns();

    // A call yielding nothing produces no column, so it has no row count for a carried
    // row to be replicated against - it is called for what it does, not for rows, and
    // nothing can ride through it.
    if (yields.empty() && !carriedColumns.empty()) {
        return emitOpError("yields no return value, so it cannot carry ")
               << carriedColumns.size() << " columns past it";
    }

    const Operation::result_range results = getResults();
    const size_t expectedResults = yields.size() + carriedColumns.size();
    if (results.size() != expectedResults) {
        return emitOpError("expects ") << expectedResults
                                       << " results, one per yielded return value and carried column, but has "
                                       << results.size();
    }

    // A carried column is only replicated by the call, never retyped, so each trailing
    // result keeps its carried column's type - the pass-through check db.limit shares,
    // applied to the results after the yields.
    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        const Value carried = carriedColumns[carriedIndex];
        const Value result = results[yields.size() + carriedIndex];

        if (carried.getType() != result.getType()) {
            return emitOpError("carried result ") << carriedIndex
                                                  << " must have the same type as carried column "
                                                  << carriedIndex;
        }
    }

    return success();
}

// Allows inline declaration of a constant type
LogicalResult ConstantOp::inferReturnTypes(MLIRContext* context,
                                           std::optional<Location> location,
                                           ValueRange operands,
                                           DictionaryAttr attributes,
                                           PropertyRef properties,
                                           RegionRange regions,
                                           SmallVectorImpl<Type>& inferredReturnTypes) {
    ConstantOpGenericAdaptor adaptor(operands, attributes, properties, regions);

    // An array of per-element attributes is a list literal; it carries no type of its own,
    // so the element type is the homogeneity verdict over the elements.
    if (const auto elements = llvm::dyn_cast<ArrayAttr>(adaptor.getValue())) {
        const mlir::Type shared = ::db::sharedLiteralElementType(elements);
        const mlir::Type listElement = shared ? shared : storage::ListElementType::get(context);
        const mlir::Type listType = storage::ListType::get(context, listElement);

        inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, listType));
        return success();
    }

    // A dense f32 array is an embedding: a flat run of floats, which is the one attribute
    // kind carrying no type of its own that still names its column's element type.
    if (llvm::isa<mlir::DenseF32ArrayAttr>(adaptor.getValue())) {
        const mlir::Type embeddingType = storage::EmbeddingType::get(context);

        inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, embeddingType));
        return success();
    }

    if (llvm::isa<mlir::DictionaryAttr>(adaptor.getValue())) {
        const mlir::Type mapType = storage::MapType::get(context);

        inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, mapType));
        return success();
    }

    // Type inference runs while the op is still being parsed, ahead of the operand
    // constraint that limits the value to those three kinds, so an attribute of any other
    // kind has to be turned away here rather than cast blindly.
    const mlir::TypedAttr typedValue = llvm::dyn_cast<mlir::TypedAttr>(adaptor.getValue());
    if (!typedValue) {
        return mlir::emitOptionalError(location,
                                       "db.constant carries a typed value, a dense f32 array "
                                       "or an array of literals, not ", adaptor.getValue());
    }

    inferredReturnTypes.emplace_back(mlir::db::ColumnType::get(context, typedValue.getType()));
    return mlir::success();
}

// db.group_aggregate splits its columns into keyCount grouping keys followed by
// one input per aggregate, so keyCount and kinds must partition the columns with
// at least one of each. Its results are the key columns (passed through) then one
// per aggregate, so the key results must match their key columns; the aggregate
// result types are resolved during lowering and left unconstrained (as db.count).
LogicalResult GroupAggregate::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();
    const uint64_t keyCount = getKeyCount();
    const ArrayRef<int64_t> kinds = getKinds();

    // A grouped aggregate with no key is a whole-stream aggregate (db.count /
    // db.sum ...); with no aggregate it is a projection or DISTINCT. Either way it
    // is not this op, so require at least one of each.
    if (keyCount == 0) {
        return emitOpError("requires at least one grouping key");
    }

    if (kinds.empty()) {
        return emitOpError("requires at least one aggregate");
    }

    // Every column is either a grouping key or one aggregate's input, so the key and
    // aggregate counts must partition the columns exactly. keyCount is an unbounded
    // attribute, so bound it by the real column count first and compare the remainder
    // to kinds.size(): summing keyCount + kinds.size() would wrap in unsigned 64-bit
    // and let a pathological keyCount slip past the check into out-of-bounds lowering.
    const size_t columnCount = columns.size();
    if (keyCount > columnCount || columnCount - keyCount != kinds.size()) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns and " << kinds.size()
                                       << " aggregate columns, but has " << columnCount;
    }

    // One result per grouping key then one per aggregate.
    if (results.size() != columns.size()) {
        return emitOpError("expects ") << columns.size()
                                       << " results, one per grouping key and aggregate, but has "
                                       << results.size();
    }

    // The grouping keys pass through unchanged, so each key result keeps its key
    // column's type. The aggregate result types are resolved during lowering.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        if (columns[keyIndex].getType() != results[keyIndex].getType()) {
            return emitOpError("grouping-key result ") << keyIndex
                                                       << " must have the same type as key column "
                                                       << keyIndex;
        }
    }

    // Each kind names one aggregate's reduction, so it must be a valid
    // GroupAggregateKind (count / count_distinct / sum / sum_distinct / min / max /
    // avg / avg_distinct).
    for (const int64_t kind : kinds) {
        if (!storage::symbolizeGroupAggregateKind(kind)) {
            return emitOpError("has an unknown aggregate kind ") << kind;
        }
    }

    return success();
}

// db.collect splits its columns into keyCount grouping keys followed by exactly one
// collected value column, so operands are keyCount + 1. Its results are the key
// columns (passed through) then one list column holding the collected values.
LogicalResult Collect::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();
    const uint64_t keyCount = getKeyCount();

    // The collected columns follow the grouping keys, and one aggregate input follows
    // them per kind, so what is left over after the keys and the aggregates is the value
    // columns - at least one. Bound keyCount by the real column count first: summing
    // keyCount + aggregateCount could wrap in unsigned 64-bit and let a pathological
    // keyCount slip past into out-of-bounds lowering.
    const size_t columnCount = columns.size();
    const llvm::ArrayRef<int64_t> kinds = getKinds().value_or(llvm::ArrayRef<int64_t> {});
    const size_t aggregateCount = kinds.size();
    if (keyCount >= columnCount || columnCount - keyCount <= aggregateCount) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns, at least one collected column and "
                                       << aggregateCount
                                       << " aggregate columns, but has "
                                       << columnCount;
    }

    const size_t valueCount = columnCount - keyCount - aggregateCount;

    // One result per grouping key, one per collected list, then one per aggregate.
    if (results.size() != columnCount) {
        return emitOpError("expects ") << columnCount
                                       << " results, one per grouping key plus the collected lists and the "
                                          "aggregates, but has "
                                       << results.size();
    }

    // The grouping keys pass through unchanged, so each key result keeps its key
    // column's type.
    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        if (columns[keyIndex].getType() != results[keyIndex].getType()) {
            return emitOpError("grouping-key result ") << keyIndex
                                                       << " must have the same type as key column "
                                                       << keyIndex;
        }
    }

    // Each value's result is the list it collects into: a column whose element type is a
    // storage list of that value column's type.
    for (size_t valueIndex = 0; valueIndex < valueCount; valueIndex++) {
        const size_t position = keyCount + valueIndex;

        const ColumnType listColumn = llvm::dyn_cast<ColumnType>(results[position].getType());
        if (!listColumn || !llvm::isa<storage::ListType>(listColumn.getType())) {
            return emitOpError("collected result ") << valueIndex << " must be a list column";
        }

        const ColumnType valueColumn = llvm::cast<ColumnType>(columns[position].getType());
        const storage::ListType collectedList = llvm::cast<storage::ListType>(listColumn.getType());
        if (collectedList.getElementType() != valueColumn.getType()) {
            return emitOpError("collected result ") << valueIndex
                                                    << " must be a list of "
                                                    << valueColumn.getType()
                                                    << ", the collected column's type, but collects "
                                                    << collectedList.getElementType();
        }
    }

    // Each kind names one aggregate's reduction, so it must be a valid
    // GroupAggregateKind, exactly as db.group_aggregate's kinds must.
    for (const int64_t kind : kinds) {
        if (!storage::symbolizeGroupAggregateKind(kind)) {
            return emitOpError("has an unknown aggregate kind ") << kind;
        }
    }

    // Every deduplicating value names one of the collected columns.
    if (const std::optional<llvm::ArrayRef<int64_t>> distinctValues = getDistinctValues()) {
        for (const int64_t valueIndex : *distinctValues) {
            if (valueIndex < 0 || static_cast<size_t>(valueIndex) >= valueCount) {
                return emitOpError("distinct value index ") << valueIndex
                                                            << " is out of range for "
                                                            << valueCount
                                                            << " collected columns";
            }
        }
    }

    return success();
}

// db.unwind_collect has the same column shape as db.collect - keyCount grouping keys
// then one collected value column - but re-emits one scalar row per element, so its
// results are the key columns (passed through) then one value column.
LogicalResult UnwindCollect::verify() {
    const OperandRange columns = getColumns();
    const Operation::result_range results = getResults();
    const uint64_t keyCount = getKeyCount();

    const size_t columnCount = columns.size();
    if (keyCount >= columnCount || columnCount - keyCount != 1) {
        return emitOpError("expects ") << keyCount
                                       << " grouping-key columns and one collected column, but has "
                                       << columnCount;
    }

    if (results.size() != columnCount) {
        return emitOpError("expects ") << columnCount
                                       << " results, one per grouping key plus the unwound value, but has "
                                       << results.size();
    }

    for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
        if (columns[keyIndex].getType() != results[keyIndex].getType()) {
            return emitOpError("grouping-key result ") << keyIndex
                                                       << " must have the same type as key column "
                                                       << keyIndex;
        }
    }

    if (columns[keyCount].getType() != results[keyCount].getType()) {
        return emitOpError("unwound value result must have the same type as the collected column");
    }

    return success();
}

// A carried column comes back replicated, never retyped, so the carry set and the
// results past the element column must match one for one.
LogicalResult Unwind::verify() {
    const OperandRange carriedColumns = getColumnsToFilter();
    const Operation::result_range carriedResults = getCarried();

    if (carriedColumns.size() != carriedResults.size()) {
        return emitOpError("expects one carried result per carried column, but has ")
               << carriedResults.size() << " for " << carriedColumns.size();
    }

    for (size_t carriedIndex = 0; carriedIndex < carriedColumns.size(); carriedIndex++) {
        if (carriedColumns[carriedIndex].getType() != carriedResults[carriedIndex].getType()) {
            return emitOpError("carried result ") << carriedIndex
                                                  << " must have the same type as carried column "
                                                  << carriedIndex;
        }
    }

    return success();
}

// The body binds the element, the row tag and one argument per carried column, and ends
// naming what each element contributes.
LogicalResult ListComprehension::verify() {
    Block& bodyBlock = getBody().front();

    auto yield = dyn_cast_or_null<ComprehensionYield>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return emitOpError("body region must end with a db.comprehension_yield");
    }

    const mlir::OperandRange carried = getColumnsToFilter();
    const size_t expectedArguments = carried.size() + 2;

    if (bodyBlock.getNumArguments() != expectedArguments) {
        return emitOpError("body region takes the element and the row tag plus one argument per "
                           "carried column, ")
               << "expected " << expectedArguments << " but has " << bodyBlock.getNumArguments();
    }

    if (!llvm::isa<ColumnType>(bodyBlock.getArgument(0).getType())) {
        return emitOpError("body argument 0 must be the column of elements");
    }

    const auto rowTagType = llvm::dyn_cast<ColumnType>(bodyBlock.getArgument(1).getType());

    if (!rowTagType || !rowTagType.getType().isUnsignedInteger(64)) {
        return emitOpError("body argument 1 must be the ui64 column of row tags");
    }

    if (!yield.getRowTags()) {
        return emitOpError("body must yield the row tag of every surviving element");
    }

    for (size_t carriedIndex = 0; carriedIndex < carried.size(); carriedIndex++) {
        const mlir::Type argumentType = bodyBlock.getArgument(carriedIndex + 2).getType();

        if (argumentType != carried[carriedIndex].getType()) {
            return emitOpError("body argument ") << carriedIndex + 2
                                                 << " must have the type of carried column "
                                                 << carriedIndex;
        }
    }

    return success();
}

// The pattern takes one argument per input column and the row tag, and ends naming what
// each match contributes to the list of the row it came from.
LogicalResult PatternComprehension::verify() {
    Block& patternBlock = getPattern().front();

    auto yield = dyn_cast_or_null<ComprehensionYield>(patternBlock.empty() ? nullptr : &patternBlock.back());
    if (!yield) {
        return emitOpError("pattern region must end with a db.comprehension_yield");
    }

    const mlir::OperandRange inputs = getInputColumns();
    const size_t inputCount = inputs.size();

    // No input column means no input row to tag: the one list covers the single empty row
    // the query starts from, which every match belongs to.
    const size_t expectedArguments = inputCount == 0 ? 0 : inputCount + 1;

    if (patternBlock.getNumArguments() != expectedArguments) {
        return emitOpError("pattern region takes one argument per input column plus the row tag, ")
               << "expected " << expectedArguments << " but has " << patternBlock.getNumArguments();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (patternBlock.getArgument(inputIndex).getType() != inputs[inputIndex].getType()) {
            return emitOpError("pattern argument ") << inputIndex << " must have the type of input column "
                                                    << inputIndex;
        }
    }

    if (inputCount != 0) {
        const auto rowTagType =
            llvm::dyn_cast<ColumnType>(patternBlock.getArgument(inputCount).getType());

        if (!rowTagType || !rowTagType.getType().isUnsignedInteger(64)) {
            return emitOpError("pattern argument ") << inputCount << " must be the ui64 column of row tags";
        }
    }

    const bool tagsRows = inputCount != 0;
    const bool yieldsATag = yield.getRowTags() != nullptr;

    if (tagsRows != yieldsATag) {
        return emitOpError("the pattern yields a row tag exactly when the op takes input columns");
    }

    return success();
}

// There is a column for the list to be built out of, and what the op produces is a list.
LogicalResult MakeList::verify() {
    if (getElements().empty()) {
        return emitOpError("requires at least one element column");
    }

    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    if (!llvm::isa<storage::ListType>(resultColumn.getType())) {
        return emitOpError("result must be a column of lists");
    }

    return success();
}

LogicalResult MakeMap::verify() {
    const size_t valueCount = getValues().size();
    if (valueCount == 0) {
        return emitOpError("requires at least one value column");
    }

    const ArrayAttr keys = getKeys();
    if (keys.size() != valueCount) {
        return emitOpError("requires one key per value column");
    }

    llvm::SmallDenseSet<StringRef, 8> seenKeys;
    for (const Attribute key : keys) {
        if (!seenKeys.insert(llvm::cast<StringAttr>(key).getValue()).second) {
            return emitOpError("requires distinct keys");
        }
    }

    return success();
}

// The integers a range spans ride the list it builds, so the result is a column of them.
LogicalResult Range::verify() {
    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    const storage::ListType listType = llvm::dyn_cast<storage::ListType>(resultColumn.getType());
    if (!listType) {
        return emitOpError("result must be a column of lists");
    }

    if (!llvm::isa<mlir::IntegerType>(listType.getElementType())) {
        return emitOpError("result must be a column of integer lists");
    }

    return success();
}

// The unwound column's element type is the homogeneity verdict: a type-erased
// list_element column accepts any elements, a typed one requires them to share that one
// type - the shared check the const_list verifier runs too.
LogicalResult UnwindConst::verify() {
    const ColumnType resultColumn = llvm::dyn_cast<ColumnType>(getResult().getType());
    if (!resultColumn) {
        return emitOpError("result must be a column");
    }

    const mlir::Type elementType = resultColumn.getType();
    if (llvm::isa<storage::ListElementType>(elementType)) {
        return success();
    }

    return verifyHomogeneousElements(getOperation(), getElements());
}

// One column per field, and a field is named either by position or by header - a
// header only where a header line was read, since nothing else resolves the name.
LogicalResult LoadCSV::verify() {
    const ArrayAttr fields = getFieldsAttr();

    // A load producing no column produces no rows either: nothing downstream could read
    // the records it opened, and its loop would have no chunk to size a step from
    if (fields.empty()) {
        return emitOpError("requires at least one field");
    }

    if (fields.size() != getResults().size()) {
        return emitOpError("produces one column per field, but names ")
               << fields.size() << " fields for " << getResults().size() << " columns";
    }

    for (const Attribute field : fields) {
        if (const auto header = llvm::dyn_cast<StringAttr>(field)) {
            if (!getWithHeaders()) {
                return emitOpError("names field '") << header.getValue()
                                                    << "' by header, which only a with_headers load resolves";
            }
        } else {
            const auto index = llvm::dyn_cast<IntegerAttr>(field);
            if (!index || !index.getType().isUnsignedInteger(64)) {
                return emitOpError("field must be a ui64 position or a header name");
            }
        }
    }

    return success();
}

// A search reporting no neighbour, or searching for no vector, is a query that asked for
// nothing: neither is a shape the index can be asked for, so both are rejected here rather
// than left to return an empty result the query would read as "no match".
LogicalResult VectorSearch::verify() {
    if (getK() == 0) {
        return emitOpError("must report at least one neighbour");
    }

    if (getQueryVector().empty()) {
        return emitOpError("must search for a vector of at least one dimension");
    }

    return success();
}

// The region's entry block stands for the rows in flight: one argument per input
// column, then the row tag. The db.optional_yield names the tag and the columns
// the region contributes - the input columns as the pattern left them, then the
// pattern's own variables - and the op's results are exactly those columns. A
// pattern variable is what a row the pattern missed comes back with null, so it
// has to be an ID column: those carry an invalid ID, which is how a null entity
// is spelled, where a property value column would need a null of its own.
LogicalResult OptionalMatch::verify() {
    Block& patternBlock = getPattern().front();

    auto yield = dyn_cast_or_null<OptionalYield>(patternBlock.empty() ? nullptr : &patternBlock.back());
    if (!yield) {
        return emitOpError("pattern region must end with a db.optional_yield");
    }

    const size_t inputCount = getInputColumns().size();

    // No input column means no input row to tag: the rows the pattern joins onto are
    // the single empty row the query starts from.
    const size_t expectedArguments = inputCount == 0 ? 0 : inputCount + 1;

    if (patternBlock.getNumArguments() != expectedArguments) {
        return emitOpError("pattern region takes one argument per input column plus the row tag, ")
               << "expected " << expectedArguments << " but has " << patternBlock.getNumArguments();
    }

    const bool tagsRows = inputCount != 0;
    const bool yieldsATag = yield.getTag() != nullptr;

    if (tagsRows != yieldsATag) {
        return emitOpError("the pattern yields a row tag exactly when the op takes input columns");
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (patternBlock.getArgument(inputIndex).getType() != getInputColumns()[inputIndex].getType()) {
            return emitOpError("pattern argument ") << inputIndex << " must have the type of input column "
                                                    << inputIndex;
        }
    }

    const ValueRange yieldedColumns = yield.getColumns();
    if (yieldedColumns.empty()) {
        return emitOpError("pattern must yield at least one column");
    }

    if (yieldedColumns.size() < inputCount) {
        return emitOpError("pattern must yield at least one column per input column, expected ")
               << inputCount << " but has " << yieldedColumns.size();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        const mlir::Type inputType = getInputColumns()[inputIndex].getType();

        // A traversal rebinds its source to a typed ID column, so a pattern walking from a
        // column whose element type is still unspecified - what a CALL yields - hands it
        // back refined. Any other change of type is a carry set that lost track of a column.
        const auto inputColumn = llvm::dyn_cast<ColumnType>(inputType);
        const bool refinesAnUnspecifiedColumn = inputColumn
                                                && llvm::isa<mlir::NoneType>(inputColumn.getType());

        if (yieldedColumns[inputIndex].getType() != inputType && !refinesAnUnspecifiedColumn) {
            return emitOpError("yielded column ") << inputIndex << " must have the type of input column "
                                                  << inputIndex;
        }
    }

    for (size_t columnIndex = inputCount; columnIndex < yieldedColumns.size(); columnIndex++) {
        const auto column = llvm::dyn_cast<ColumnType>(yieldedColumns[columnIndex].getType());
        const mlir::Type elementType = column ? column.getType() : mlir::Type();
        const bool isEntityColumn = llvm::isa_and_present<storage::NodeIDType, storage::EdgeIDType>(elementType);

        if (!isEntityColumn) {
            return emitOpError("pattern variable column ")
                   << columnIndex << " must be a node or edge ID column, so a missed match can be null";
        }
    }

    const Operation::result_type_range resultTypes = getOperation()->getResultTypes();
    if (resultTypes.size() != yieldedColumns.size()) {
        return emitOpError("expects ") << yieldedColumns.size()
                                       << " results, the columns the pattern yields, but has "
                                       << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < resultTypes.size(); resultIndex++) {
        if (resultTypes[resultIndex] != yieldedColumns[resultIndex].getType()) {
            return emitOpError("result ") << resultIndex << " must be the yielded column type "
                                          << yieldedColumns[resultIndex].getType();
        }
    }

    return success();
}

LogicalResult ExistsSubquery::verify() {
    Block& bodyBlock = getBody().front();

    auto yield = dyn_cast_or_null<ExistsYield>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return emitOpError("body region must end with a db.exists_yield");
    }

    const OperandRange inputs = getInputColumns();
    const size_t inputCount = inputs.size();

    // The body carries the row tag through its dataflow only when it carries the scope,
    // and only over rows it has: a body run one row at a time, or over the single empty
    // row, is marked by the lowering instead.
    const bool tagsRows = getCarriesScope() && inputCount > 0;

    const size_t expectedArguments = tagsRows ? inputCount + 1 : inputCount;
    if (bodyBlock.getNumArguments() != expectedArguments) {
        return emitOpError("body region takes one argument per input column")
               << (tagsRows ? " plus the row tag" : "") << ", expected " << expectedArguments
               << " but has " << bodyBlock.getNumArguments();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (bodyBlock.getArgument(inputIndex).getType() != inputs[inputIndex].getType()) {
            return emitOpError("body argument ") << inputIndex << " must have the type of input column "
                                                 << inputIndex;
        }
    }

    const bool yieldsATag = yield.getTag() != nullptr;
    if (tagsRows != yieldsATag) {
        return emitOpError("the body yields a row tag exactly when it takes one");
    }

    // A body with no tag is answered for by the rows its columns hold, so it has to hold
    // one; a tagged body is answered for by the tag, and holds whatever its clauses left.
    if (!tagsRows && yield.getColumns().empty()) {
        return emitOpError("an untagged body must yield at least one column, to answer from its rows");
    }

    return success();
}

LogicalResult CallSubquery::verify() {
    Block& bodyBlock = getBody().front();

    auto yield = dyn_cast_or_null<SubqueryYield>(bodyBlock.empty() ? nullptr : &bodyBlock.back());
    if (!yield) {
        return emitOpError("body region must end with a db.subquery_yield");
    }

    const OperandRange inputs = getInputColumns();
    const size_t inputCount = inputs.size();

    // The body carries the row tag through its dataflow only when it carries the scope,
    // and only over rows it has: a body run one row at a time, or over the single empty
    // row, is tagged by the lowering instead.
    const bool tagsRows = getOptional() && getCarriesScope() && inputCount > 0;

    const size_t expectedArguments = tagsRows ? inputCount + 1 : inputCount;
    if (bodyBlock.getNumArguments() != expectedArguments) {
        return emitOpError("body region takes one argument per input column")
               << (tagsRows ? " plus the row tag" : "") << ", expected " << expectedArguments
               << " but has " << bodyBlock.getNumArguments();
    }

    for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
        if (bodyBlock.getArgument(inputIndex).getType() != inputs[inputIndex].getType()) {
            return emitOpError("body argument ") << inputIndex << " must have the type of input column "
                                                 << inputIndex;
        }
    }

    const bool yieldsATag = yield.getTag() != nullptr;
    if (tagsRows != yieldsATag) {
        return emitOpError("the body yields a row tag exactly when it takes one");
    }

    const ValueRange yieldedColumns = yield.getColumns();
    const Operation::result_type_range resultTypes = getOperation()->getResultTypes();

    if (getUnit()) {
        if (getOptional()) {
            return emitOpError("a unit subquery yields no row for OPTIONAL to pad");
        }

        if (!yieldedColumns.empty()) {
            return emitOpError("a unit body yields no column");
        }

        if (resultTypes.size() != 0) {
            return emitOpError("a unit subquery has no result");
        }

        return success();
    }

    // The columns the results stand for: what the body yields when it carries the scope
    // itself, and the inputs ahead of what it yields otherwise.
    llvm::SmallVector<Type, 8> expectedResults;
    if (getCarriesScope()) {
        if (yieldedColumns.size() < inputCount) {
            return emitOpError("a body carrying its scope yields at least one column per input column, expected ")
                   << inputCount << " but has " << yieldedColumns.size();
        }

        for (size_t inputIndex = 0; inputIndex < inputCount; inputIndex++) {
            const Type inputType = inputs[inputIndex].getType();

            // A traversal rebinds its source to a typed ID column, so a body walking from a
            // column whose element type is still unspecified hands it back refined.
            const auto inputColumn = llvm::dyn_cast<ColumnType>(inputType);
            const bool refinesAnUnspecifiedColumn = inputColumn && llvm::isa<mlir::NoneType>(inputColumn.getType());

            if (yieldedColumns[inputIndex].getType() != inputType && !refinesAnUnspecifiedColumn) {
                return emitOpError("yielded column ") << inputIndex << " must have the type of input column "
                                                      << inputIndex;
            }
        }
    } else {
        for (const Value input : inputs) {
            expectedResults.push_back(input.getType());
        }
    }

    for (const Value yielded : yieldedColumns) {
        expectedResults.push_back(yielded.getType());
    }

    if (resultTypes.size() != expectedResults.size()) {
        return emitOpError("expects ") << expectedResults.size()
                                       << " results, one per input column then one per yielded column, but has "
                                       << resultTypes.size();
    }

    for (size_t resultIndex = 0; resultIndex < expectedResults.size(); resultIndex++) {
        if (resultTypes[resultIndex] != expectedResults[resultIndex]) {
            return emitOpError("result ") << resultIndex << " must have type " << expectedResults[resultIndex];
        }
    }

    return success();
}
