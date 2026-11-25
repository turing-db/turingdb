#include "DBPasses.h"

#include <stdint.h>
#include <numeric>
#include <optional>

#include "mlir/IR/Builders.h"
#include "mlir/IR/Operation.h"
#include "mlir/IR/ValueRange.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallVector.h"

#include "DBOps.h"
#include "DBCarriedColumns.h"
#include "DBPassUtils.h"

namespace mlir::db {

#define GEN_PASS_DEF_SINKMAKEPATH
#define GEN_PASS_DEF_SINKINTOOPTIONALMATCH
#define GEN_PASS_DEF_FUSEPATHELEMENTS
#define GEN_PASS_DEF_FUSEPATHLENGTH
#define GEN_PASS_DEF_COUNTPATHROWS
#include "DBPasses.h.inc"

namespace {

// A filter carrying a named path cuts the rows before the path is built: the path is built
// again behind it, over the columns the filter keeps. A mask reading the path still reads
// the build ahead of the filter. Null where the filter does not keep every entity.
MakePath sinkPastFilter(MakePath build, FilterOp filter, mlir::OpBuilder& builder) {
    const Value path = build.getResult();
    const OperandRange carried = filter.getColumnsToFilter();

    llvm::SmallVector<Value> keptColumns;
    llvm::SmallVector<Type> keptTypes;
    llvm::SmallVector<size_t> keptIndices(carried.size(), 0);

    for (size_t index = 0; index < carried.size(); index++) {
        const Value column = carried[index];
        if (column == path) {
            continue;
        }

        keptIndices[index] = keptColumns.size();
        keptColumns.push_back(column);
        keptTypes.push_back(column.getType());
    }

    llvm::SmallVector<size_t> entityIndices;
    for (const Value entity : build.getEntities()) {
        const auto keptIt = llvm::find(keptColumns, entity);
        if (keptIt == keptColumns.end()) {
            return nullptr;
        }

        entityIndices.push_back(static_cast<size_t>(keptIt - keptColumns.begin()));
    }

    builder.setInsertionPoint(filter);
    FilterOp narrowed = builder.create<FilterOp>(filter.getLoc(), keptTypes, filter.getMask(), keptColumns);

    llvm::SmallVector<Value> entities;
    for (const size_t entityIndex : entityIndices) {
        entities.push_back(narrowed.getResult(entityIndex));
    }

    builder.setInsertionPointAfter(narrowed);
    MakePath sunk = builder.create<MakePath>(build.getLoc(), path.getType(), entities, build.getReversedPathsAttr());

    const ResultRange filtered = filter.getFilteredColumns();
    for (size_t index = 0; index < filtered.size(); index++) {
        const Value replacement = carried[index] == path ? sunk.getResult() : narrowed.getResult(keptIndices[index]);
        filtered[index].replaceAllUsesWith(replacement);
    }

    filter.erase();

    return sunk;
}

MakePath sinkPastFirstSinkableFilter(MakePath build, mlir::OpBuilder& builder) {
    for (Operation* user : build.getResult().getUsers()) {
        FilterOp filter = dyn_cast<FilterOp>(user);
        if (!filter || filter->getBlock() != build->getBlock()) {
            continue;
        }

        if (const MakePath sunk = sinkPastFilter(build, filter, builder)) {
            return sunk;
        }
    }

    return nullptr;
}

struct SinkMakePath : public impl::SinkMakePathBase<SinkMakePath> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<MakePath> builds;
        getOperation()->walk([&builds](MakePath build) {
            builds.push_back(build);
        });

        while (!builds.empty()) {
            MakePath build = builds.pop_back_val();
            const MakePath sunk = sinkPastFirstSinkableFilter(build, builder);
            if (!sunk) {
                continue;
            }

            builds.push_back(sunk);

            if (build->use_empty()) {
                build.erase();
            } else {
                builds.push_back(build);
            }
        }
    }
};

// A missed row pads a pattern variable with null, and size(), nodes(), relationships() and
// length() of a null are null too. So the pattern computes them before the padding, which
// then carries their result instead of the list or path.
bool matchOptionalVariableRead(Operation* read, OptionalMatch& optional, size_t& resultIndex) {
    if (!isa<Size, PathElements>(read)) {
        return false;
    }

    const OpResult padded = dyn_cast<OpResult>(read->getOperand(0));
    if (!padded) {
        return false;
    }

    optional = dyn_cast<OptionalMatch>(padded.getOwner());
    resultIndex = padded.getResultNumber();

    return optional && resultIndex >= optional.getInputColumns().size();
}

void sinkIntoOptionalMatch(Operation* read, OptionalMatch optional, size_t resultIndex, mlir::OpBuilder& builder) {
    OptionalYield yield = patternYield(optional);

    builder.setInsertionPoint(yield);
    Operation* const sunk = builder.clone(*read);
    sunk->setOperand(0, yield.getColumns()[resultIndex]);

    yield.getColumnsMutable().append(sunk->getResult(0));

    llvm::SmallVector<size_t> keptResults(optional->getNumResults());
    std::iota(keptResults.begin(), keptResults.end(), size_t {0});

    OptionalMatch rebuilt = rebuildOptionalMatch(optional, keptResults, builder);

    read->getResult(0).replaceAllUsesWith(rebuilt->getResults().back());
    read->erase();
}

struct SinkIntoOptionalMatch : public impl::SinkIntoOptionalMatchBase<SinkIntoOptionalMatch> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SetVector<Operation*> reads;
        getOperation()->walk([&reads](Operation* op) {
            if (isa<Size, PathElements>(op)) {
                reads.insert(op);
            }
        });

        while (!reads.empty()) {
            Operation* const read = reads.pop_back_val();

            OptionalMatch optional;
            size_t resultIndex = 0;
            if (!matchOptionalVariableRead(read, optional, resultIndex)) {
                continue;
            }

            const Operation::user_range users = read->getUsers();
            const llvm::SmallVector<Operation*> readers(users.begin(), users.end());
            sinkIntoOptionalMatch(read, optional, resultIndex, builder);
            reads.insert(readers.begin(), readers.end());
        }
    }
};

// A named path over one walk is the node the walk seeds from and its handle column, so
// what nodes(), relationships() and length() read off it is what the handles already
// answer: the seed then each hop's end, the edges, and the depth
struct WalkPath {
    MakePath _build;
    Value _seed;
    Value _path;
    bool _reversed {false};
};

bool matchWalkPath(PathElements elements, WalkPath& walk) {
    MakePath build = elements.getPath().getDefiningOp<MakePath>();
    if (!build) {
        return false;
    }

    const OperandRange entities = build.getEntities();
    if (entities.size() != 2) {
        return false;
    }

    const std::optional<ArrayAttr> reversedPaths = build.getReversedPaths();
    const bool reversed = reversedPaths && !reversedPaths->empty();

    const Value seed = reversed ? entities[1] : entities[0];
    const Value path = reversed ? entities[0] : entities[1];

    const auto elementOf = [](Value value) { return cast<ColumnType>(value.getType()).getType(); };
    if (!isa<storage::NodeIDType>(elementOf(seed)) || !isa<storage::PathRefType>(elementOf(path))) {
        return false;
    }

    walk = WalkPath {._build = build, ._seed = seed, ._path = path, ._reversed = reversed};

    return true;
}

Value walkPathElements(PathElements elements, const WalkPath& walk, mlir::OpBuilder& builder) {
    MLIRContext* const context = builder.getContext();
    const Location loc = elements.getLoc();

    switch (elements.getKind()) {
        case storage::PathElementsKind::Length: {
            const Type countType = ColumnType::get(context, IntegerType::get(context, 64, IntegerType::Unsigned));
            return builder.create<PathLength>(loc, countType, walk._path).getResult();
        }
        break;

        case storage::PathElementsKind::Relationships: {
            const Type listType = ColumnType::get(context, storage::ListType::get(context, storage::EdgeIDType::get(context)));
            return builder.create<ExpandPath>(loc,
                                              listType,
                                              walk._path,
                                              Value(),
                                              storage::PathExpansionKind::Edges,
                                              walk._reversed,
                                              IntegerAttr(),
                                              IntegerAttr()).getResult();
        }
        break;

        case storage::PathElementsKind::Nodes: {
            const Type listType = ColumnType::get(context, storage::ListType::get(context, storage::NodeIDType::get(context)));
            return builder.create<ExpandPath>(loc,
                                              listType,
                                              walk._path,
                                              walk._seed,
                                              storage::PathExpansionKind::Nodes,
                                              walk._reversed,
                                              IntegerAttr(),
                                              IntegerAttr()).getResult();
        }
        break;
    }

    return Value();
}

struct FusePathElements : public impl::FusePathElementsBase<FusePathElements> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<PathElements> reads;
        getOperation()->walk([&reads](PathElements elements) {
            reads.push_back(elements);
        });

        for (PathElements elements : reads) {
            WalkPath walk;
            if (!matchWalkPath(elements, walk)) {
                continue;
            }

            builder.setInsertionPoint(elements);
            const Value handles = walkPathElements(elements, walk, builder);

            Value result = elements.getResult();
            ToNullable widened = builder.create<ToNullable>(elements.getLoc(), result.getType(), handles);

            result.replaceAllUsesWith(widened.getResult());
            elements.erase();
            eraseIfUnused(walk._build);
        }
    }
};

// A to_nullable column holds a value in every row, so the size of one is the size of the
// column it widens
void readSizeOfTheWidenedColumn(Size size) {
    while (ToNullable widened = size.getInput().getDefiningOp<ToNullable>()) {
        size->setOperand(0, widened.getOperand());
        eraseIfUnused(widened);
    }
}

// The list a walk's handles expand to holds one entry per hop, or per repetition of a body
// of several steps, which the depth of the handle answers, and its list of nodes one more
struct FusePathLength : public impl::FusePathLengthBase<FusePathLength> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<Size> sizes;
        getOperation()->walk([&sizes](Size size) {
            sizes.push_back(size);
        });

        for (Size size : sizes) {
            readSizeOfTheWidenedColumn(size);

            ExpandPath expansion = size.getInput().getDefiningOp<ExpandPath>();
            if (!expansion) {
                continue;
            }

            Value result = size.getResult();
            const bool countsNodes = expansion.getKind() == storage::PathExpansionKind::Nodes;

            builder.setInsertionPoint(size);
            PathLength length = builder.create<PathLength>(size.getLoc(),
                                                           result.getType(),
                                                           expansion.getPaths(),
                                                           expansion.getStepsAttr(),
                                                           countsNodes ? builder.getUnitAttr() : UnitAttr());

            result.replaceAllUsesWith(length.getResult());
            size.erase();
            eraseIfUnused(expansion);
        }
    }
};

// A count reads no value off a path, and a path is never null, so count(p) tallies the
// rows its build ran over. The tally moves onto the node the path opens on, which the
// build already read: row aligned with the path and never null in its own right.
bool matchBuiltPath(Count count, Value& rows) {
    if (count.getDistinct() || count.getRows()) {
        return false;
    }

    MakePath build = dyn_cast_or_null<MakePath>(count.getInput().getDefiningOp());
    if (!build) {
        return false;
    }

    rows = build.getEntities().front();

    return true;
}

// The list a walk's handles expand to is never null either, so a count of a path or of
// that list tallies the rows of a column its build read: the node the path opens on, or
// the handles themselves
void countBuiltRows(OpOperand& counted) {
    Operation* const build = counted.get().getDefiningOp();

    if (MakePath path = dyn_cast_or_null<MakePath>(build)) {
        counted.set(path.getEntities().front());
    } else if (ExpandPath expansion = dyn_cast_or_null<ExpandPath>(build)) {
        counted.set(expansion.getPaths());
    } else {
        return;
    }

    eraseIfUnused(build);
}

// The aggregate inputs of a grouping op are its trailing operands, one per kind
void countGroupedBuiltRows(Operation* grouping, ArrayRef<int64_t> kinds) {
    const size_t firstInput = grouping->getNumOperands() - kinds.size();
    const int64_t countKind = static_cast<int64_t>(storage::GroupAggregateKind::Count);

    for (size_t index = 0; index < kinds.size(); index++) {
        if (kinds[index] == countKind) {
            countBuiltRows(grouping->getOpOperand(firstInput + index));
        }
    }
}

struct CountPathRows : public impl::CountPathRowsBase<CountPathRows> {
    void runOnOperation() override {
        mlir::OpBuilder builder(&getContext());

        llvm::SmallVector<Count> counts;
        llvm::SmallVector<GroupAggregate> groupAggregates;
        llvm::SmallVector<Collect> collects;
        getOperation()->walk([&counts, &groupAggregates, &collects](Operation* op) {
            if (Count count = dyn_cast<Count>(op)) {
                counts.push_back(count);
            } else if (GroupAggregate groupAggregate = dyn_cast<GroupAggregate>(op)) {
                groupAggregates.push_back(groupAggregate);
            } else if (Collect collect = dyn_cast<Collect>(op)) {
                collects.push_back(collect);
            }
        });

        for (Count count : counts) {
            Value rows;
            if (matchBuiltPath(count, rows)) {
                Operation* build = count.getInput().getDefiningOp();

                count->setOperand(0, rows);
                count.setRowsAttr(builder.getUnitAttr());

                if (build->use_empty()) {
                    build->erase();
                }
            } else if (!count.getDistinct()) {
                countBuiltRows(count->getOpOperand(0));
            }
        }

        for (GroupAggregate groupAggregate : groupAggregates) {
            countGroupedBuiltRows(groupAggregate, groupAggregate.getKinds());
        }

        for (Collect collect : collects) {
            const std::optional<ArrayRef<int64_t>> kinds = collect.getKinds();
            if (kinds) {
                countGroupedBuiltRows(collect, *kinds);
            }
        }
    }
};

}

}
