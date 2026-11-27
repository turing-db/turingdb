#include "DBPasses.h"

#include <algorithm>
#include <tuple>

#include "mlir/IR/Operation.h"
#include "mlir/IR/PatternMatch.h"
#include "mlir/IR/ValueRange.h"
#include "mlir/Transforms/RegionUtils.h"

#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/DenseSet.h"
#include "llvm/ADT/SetVector.h"
#include "llvm/ADT/SmallPtrSet.h"
#include "llvm/ADT/SmallVector.h"

#include "IRConstantColumn.h"
#include "IRRaiseEffect.h"
#include "DBOps.h"
#include "DBCarriedColumns.h"
#include "DBPassUtils.h"

#include "BioAssert.h"

namespace mlir::db {

#define GEN_PASS_DEF_REUSEPROPERTYREADS
#define GEN_PASS_DEF_REUSELABELANDTYPEREADS
#define GEN_PASS_DEF_CARRYLABELANDTYPEREADS
#define GEN_PASS_DEF_HOISTREPEATEDREGIONINVARIANTOPS
#include "DBPasses.h.inc"

namespace {

// The argument @param target, a region after @param source, takes @param column of
// @param source as. A column @param source takes itself comes into @param target the same
// way; any other @param source forwards, as an argument of every region after it.
BlockArgument forwardColumn(Region& source, Value column, Region& target, mlir::RewriterBase& rewriter) {
    Block& targetBody = target.front();

    BlockArgument argument = dyn_cast<BlockArgument>(column);
    if (argument && argument.getParentRegion() == &source) {
        Region* forwardingSource = nullptr;
        size_t forwardIndex = 0;
        if (matchForwardedArgument(argument, forwardingSource, forwardIndex)) {
            return targetBody.getArgument(static_cast<unsigned>(forwardedArgumentCount(*forwardingSource) + forwardIndex));
        }

        RegionCarrySet carrySet;
        matchRegionCarrySet(source, carrySet);

        return carriedArgument(target, argument.getArgNumber() - carrySet._argumentOffset);
    }

    Operation* const op = source.getParentOp();
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);
    MutableOperandRange forwarded = carrier.getForwardedColumnsMutable(source);

    const size_t forwardOffset = forwardedArgumentCount(source);
    const OperandRange current = forwarded;
    const size_t forwardIndex = static_cast<size_t>(llvm::find(current, column) - current.begin());

    if (forwardIndex == current.size()) {
        const unsigned position = static_cast<unsigned>(forwardOffset + forwardIndex);
        Operation* const yield = source.front().getTerminator();

        rewriter.modifyOpInPlace(yield, [&forwarded, column] {
            forwarded.append(column);
        });

        for (size_t regionIndex = source.getRegionNumber() + 1; regionIndex < op->getNumRegions(); regionIndex++) {
            Block& later = op->getRegion(static_cast<unsigned>(regionIndex)).front();
            later.insertArgument(position, column.getType(), column.getLoc());
        }
    }

    return targetBody.getArgument(static_cast<unsigned>(forwardOffset + forwardIndex));
}

// The ops whose results hold the rows of their operands selected, reordered or repeated as
// a whole, values untouched, so a property read of an operand and carried through is the
// column a read of the result would have produced. group_aggregate and collect are not
// among them - a group's rows are not its input's - and neither is remove_duplicates,
// whose dedup key is every column it carries, so a wider carry set drops different rows.
bool mapsRowsThrough(Operation* op) {
    return isEdgeHop(op) || isa<FilterOp, ExplorePaths, Unwind, Limit, Skip, Sort>(op);
}

bool matchPropertyRead(Operation* op, StringAttr& property, bool& nodeProperty) {
    if (GetNodeProperties read = dyn_cast<GetNodeProperties>(op)) {
        property = read.getPropertyAttr();
        nodeProperty = true;
        return true;
    } else if (GetEdgeProperties read = dyn_cast<GetEdgeProperties>(op)) {
        property = read.getPropertyAttr();
        nodeProperty = false;
        return true;
    }

    return false;
}

// What a read takes off each row of a node or an edge column: the op reading it, and the
// property it names, null for the label sets of a node or the types of an edge
struct EntityRead {
    TypeID _op;
    StringAttr _property;

    bool operator==(const EntityRead& other) const = default;
};

EntityRead entityReadOf(Operation* read) {
    StringAttr property;
    bool nodeProperty = false;
    matchPropertyRead(read, property, nodeProperty);

    return EntityRead {._op = read->getName().getTypeID(), ._property = property};
}

bool matchPropertyWrite(Operation* op, StringAttr& property, bool& nodeProperty) {
    if (SetNodeProperty write = dyn_cast<SetNodeProperty>(op)) {
        property = write.getPropertyAttr();
        nodeProperty = true;
        return true;
    } else if (SetEdgeProperty write = dyn_cast<SetEdgeProperty>(op)) {
        property = write.getPropertyAttr();
        nodeProperty = false;
        return true;
    }

    return false;
}

bool matchPropertiesWrite(Operation* op, bool& nodeProperty) {
    if (isa<SetNodeProperties>(op)) {
        nodeProperty = true;
        return true;
    } else if (isa<SetEdgeProperties>(op)) {
        nodeProperty = false;
        return true;
    }

    return false;
}

Value carryThrough(Operation* op, Value column, mlir::RewriterBase& rewriter) {
    CarrySet carrySet;
    const bool carries = matchCarrySet(op, carrySet);
    bioassert(carries, "A row-mapping op has a carry set");

    for (size_t carriedIndex = 0; carriedIndex < carrySet._count; carriedIndex++) {
        if (op->getOperand(carrySet._operandOffset + carriedIndex) == column) {
            return op->getResult(carrySet._resultOffset + carriedIndex);
        }
    }

    return appendCarriedColumn(op, column, rewriter);
}

Value canonicalArgument(Value rows) {
    BlockArgument argument = dyn_cast<BlockArgument>(rows);
    const Value carried = argument ? carriedColumnOf(argument) : Value();
    if (!carried) {
        return rows;
    }

    Region& region = *argument.getParentRegion();

    RegionCarrySet carrySet;
    matchRegionCarrySet(region, carrySet);

    const OperandRange carriedColumns = region.getParentOp()->getOperands().slice(carrySet._operandOffset, carrySet._count);
    const size_t firstIndex = static_cast<size_t>(llvm::find(carriedColumns, carried) - carriedColumns.begin());

    return carriedArgument(region, firstIndex);
}

// Two columns hold the same values row for row where they come from the same column: two
// arguments of a body carrying one column in, and two results of an op mapping rows through,
// as a hop hands back the node it walks from and carries it where the rows in flight hold it.
// Both are known as the first of them.
Value canonicalRows(Value rows) {
    const OpResult result = dyn_cast<OpResult>(rows);
    Operation* const owner = result ? result.getOwner() : nullptr;
    if (!owner || !mapsRowsThrough(owner)) {
        return canonicalArgument(rows);
    }

    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(owner);
    const Value rowSource = carrier.getRowSource(result);
    if (!rowSource) {
        return rows;
    }

    const Value sourceRows = canonicalArgument(rowSource);
    const auto sibling = llvm::find_if(owner->getResults(), [&carrier, sourceRows](OpResult other) {
        const Value otherSource = carrier.getRowSource(other);
        return otherSource && canonicalArgument(otherSource) == sourceRows;
    });

    return *sibling;
}

// The column the op making @param rows outputs beside them holding what @param entityRead
// takes off each of their rows, null where it outputs none
Value outputReadColumnOf(Value rows, const EntityRead& entityRead) {
    const OpResult result = dyn_cast<OpResult>(rows);
    EdgeTypesOutputOpInterface producer = result ? dyn_cast<EdgeTypesOutputOpInterface>(result.getOwner()) : nullptr;

    const bool readsTheTypes = entityRead == EntityRead {._op = TypeID::get<GetEdgeTypes>()};
    if (!producer || !readsTheTypes) {
        return {};
    }

    return producer.getEdgeTypesOf(result);
}

// The columns reuse_property_reads and carry_label_and_type_reads can hand a later read of
// the same rows: the reads they kept, each serving the ops after it in its block, and the
// columns they carried down to a row column or the op making the rows outputs beside them,
// which serve every op reading those rows. Keyed by the row column, so a read finds an
// earlier one without visiting every other user of its rows
class KnownReadColumns {
public:
    Value find(Value rows, const EntityRead& entityRead, Operation* useSite) const {
        const Value canonical = canonicalRows(rows);
        if (const Value output = outputReadColumnOf(canonical, entityRead)) {
            return output;
        }

        const auto rowsIt = _columns.find(canonical);
        if (rowsIt == _columns.end()) {
            return {};
        }

        Value carriedColumn;
        for (const KnownColumn& known : rowsIt->second) {
            if (known._entityRead != entityRead) {
                continue;
            }

            if (!known._read) {
                carriedColumn = carriedColumn ? carriedColumn : known._column;
                continue;
            }

            const bool standsBeforeTheUse = known._read->getBlock() == useSite->getBlock()
                                            && known._read->isBeforeInBlock(useSite);
            if (standsBeforeTheUse) {
                return known._column;
            }
        }

        return carriedColumn;
    }

    void addRead(Operation* read) {
        _columns[canonicalRows(read->getOperand(0))].push_back(KnownColumn {entityReadOf(read), read->getResult(0), read});
    }

    void addCarried(Value rows, const EntityRead& entityRead, Value column) {
        _columns[canonicalRows(rows)].push_back(KnownColumn {entityRead, column, nullptr});
    }

    // Moves what is known of @param replaced, the results of an op since rebuilt, onto
    // @param replacements, the results standing for them in the op rebuilt
    void rebind(llvm::ArrayRef<Value> replaced, llvm::ArrayRef<Value> replacements) {
        llvm::DenseMap<Value, Value> replacementOf;
        for (size_t index = 0; index < replaced.size(); index++) {
            replacementOf[replaced[index]] = replacements[index];
        }

        for (size_t index = 0; index < replaced.size(); index++) {
            const auto rowsIt = _columns.find(replaced[index]);
            if (rowsIt == _columns.end()) {
                continue;
            }

            llvm::SmallVector<KnownColumn, 1> known = rowsIt->second;
            _columns.erase(rowsIt);

            for (KnownColumn& column : known) {
                const auto columnIt = replacementOf.find(column._column);
                if (columnIt != replacementOf.end()) {
                    column._column = columnIt->second;
                }
            }

            _columns[replacements[index]] = known;
        }
    }

private:
    struct KnownColumn {
        EntityRead _entityRead;
        Value _column;
        Operation* _read {nullptr};
    };

    llvm::DenseMap<Value, llvm::SmallVector<KnownColumn, 1>> _columns;
};

enum class RowsStepKind {
    MapsRows,
    IntoRegion,
    FromForwardingRegion,
    ReadsRowConstant,
    LaysOutConstant,
    OutOfRegion,
    RunConstantOutOfRegion,
    HandsBack,
};

// The rows _rows come from _outerRows across one op: _op maps them through or reads them as a
// run constant, or they come into _body, out of its yield, out of the op holding it, or out of
// _body, a region before theirs that forwards columns. Into the body, from a forwarding region
// and handed back, _position is the column's place in the op's region carry set.
struct RowsStep {
    RowsStepKind _kind {RowsStepKind::MapsRows};
    Operation* _op {nullptr};
    Block* _body {nullptr};
    size_t _position {0};
    Value _rows;
    Value _outerRows;
};

MutableOperandRange yieldedColumnsOf(Block& body) {
    Operation* const terminator = body.getTerminator();

    if (Yield yield = dyn_cast<Yield>(terminator)) {
        return yield.getColumnsMutable();
    } else if (SubqueryYield yield = dyn_cast<SubqueryYield>(terminator)) {
        return yield.getColumnsMutable();
    }

    OptionalYield yield = cast<OptionalYield>(terminator);

    return yield.getColumnsMutable();
}

bool matchMapsRowsStep(OpResult result, RowsStep& step) {
    Operation* const carrier = result.getOwner();
    if (!mapsRowsThrough(carrier)) {
        return false;
    }

    const Value rowSource = cast<ColumnCarrierOpInterface>(carrier).getRowSource(result);
    if (!rowSource) {
        return false;
    }

    step = RowsStep {._op = carrier, ._position = result.getResultNumber(), ._rows = result, ._outerRows = rowSource};

    return true;
}

bool matchHandedBackResult(OpResult result, RowsStep& step) {
    Operation* const op = result.getOwner();
    if (op->getNumRegions() == 0) {
        return false;
    }

    Region& region = op->getRegion(0);

    RegionCarrySet carrySet;
    const size_t carriedIndex = result.getResultNumber();
    const bool handsBack = matchRegionCarrySet(region, carrySet) && carrySet._handsBack && carriedIndex < carrySet._count;
    if (!handsBack) {
        return false;
    }

    step = RowsStep {._kind = RowsStepKind::HandsBack,
                     ._body = &region.front(),
                     ._position = carriedIndex,
                     ._rows = result,
                     ._outerRows = op->getOperand(carrySet._operandOffset + carriedIndex)};

    return true;
}

// The steps from the column @param body yields back for its carried column at @param carriedIndex
// down to the argument it comes in as, nearest the yield first: ops mapping rows through, and
// ops handing back a column they carry in
bool matchHandedBackChain(Block& body, size_t carriedIndex, llvm::SmallVectorImpl<RowsStep>& chain) {
    const Value argument = canonicalArgument(carriedArgument(*body.getParent(), carriedIndex));
    const OperandRange yielded = yieldedColumnsOf(body);

    Value rows = yielded[carriedIndex];
    while (canonicalArgument(rows) != argument) {
        const OpResult result = dyn_cast<OpResult>(rows);

        RowsStep step;
        const bool stepsInTheBody = result && result.getOwner()->getBlock() == &body
                                 && (matchMapsRowsStep(result, step) || matchHandedBackResult(result, step));
        if (!stepsInTheBody) {
            return false;
        }

        chain.push_back(step);
        rows = step._outerRows;
    }

    return true;
}

// Whether a column carried in alongside the one at @param carriedIndex can be handed back: along
// the steps the body hands that one back through, and through the body of each op among them
// that yields its carried columns back too
bool handsBackAlong(Block& body, const RegionCarrySet& carrySet, size_t carriedIndex) {
    if (!carrySet._yieldsBack) {
        return true;
    }

    llvm::SmallVector<std::pair<Block*, size_t>> pending {{&body, carriedIndex}};
    while (!pending.empty()) {
        const auto [handingBody, handedIndex] = pending.pop_back_val();

        llvm::SmallVector<RowsStep> chain;
        if (!matchHandedBackChain(*handingBody, handedIndex, chain)) {
            return false;
        }

        for (const RowsStep& step : chain) {
            RegionCarrySet stepCarrySet;
            const bool yieldsBack = step._kind == RowsStepKind::HandsBack
                                 && matchRegionCarrySet(*step._body->getParent(), stepCarrySet)
                                 && stepCarrySet._yieldsBack;
            if (yieldsBack) {
                pending.push_back({step._body, step._position});
            }
        }
    }

    return true;
}

bool matchRowConstantStep(OpResult result, RowsStep& step) {
    RowConstant constant = dyn_cast<RowConstant>(result.getOwner());
    if (!constant) {
        return false;
    }

    step = RowsStep {._kind = RowsStepKind::ReadsRowConstant,
                     ._op = constant.getOperation(),
                     ._rows = result,
                     ._outerRows = constant.getRow()};

    return true;
}

bool matchIntoRegionStep(BlockArgument argument, RowsStep& step) {
    const Value carried = carriedColumnOf(argument);
    if (!carried) {
        return false;
    }

    Block* const body = argument.getOwner();

    RegionCarrySet carrySet;
    matchRegionCarrySet(*body->getParent(), carrySet);

    const size_t carriedIndex = argument.getArgNumber() - carrySet._argumentOffset;
    if (!handsBackAlong(*body, carrySet, carriedIndex)) {
        return false;
    }

    step = RowsStep {._kind = RowsStepKind::IntoRegion,
                     ._body = body,
                     ._position = carriedIndex,
                     ._rows = argument,
                     ._outerRows = carried};

    return true;
}

bool matchOutOfRegionStep(OpResult result, RowsStep& step) {
    Operation* const op = result.getOwner();
    ColumnCarrierOpInterface carrier = dyn_cast<ColumnCarrierOpInterface>(op);
    if (!carrier) {
        return false;
    }

    const size_t resultIndex = result.getResultNumber();
    for (Region& region : op->getRegions()) {
        size_t resultOffset = 0;
        if (!carrier.getYieldedResultOffset(region, resultOffset)) {
            continue;
        }

        Block& body = region.front();
        const OperandRange yielded = yieldedColumnsOf(body);

        const bool comesOutOfTheRegion = resultIndex >= resultOffset && resultIndex < resultOffset + yielded.size();
        if (comesOutOfTheRegion) {
            const size_t yieldIndex = resultIndex - resultOffset;
            step = RowsStep {._kind = RowsStepKind::OutOfRegion,
                             ._body = &body,
                             ._position = yieldIndex,
                             ._rows = result,
                             ._outerRows = yielded[yieldIndex]};

            return true;
        }
    }

    return false;
}

bool matchHandsBackStep(OpResult result, RowsStep& step) {
    if (!matchHandedBackResult(result, step)) {
        return false;
    }

    Block& body = *step._body;

    RegionCarrySet carrySet;
    matchRegionCarrySet(*body.getParent(), carrySet);

    return handsBackAlong(body, carrySet, step._position);
}

Value runConstantArgument(Value column) {
    RowConstant constant = column ? column.getDefiningOp<RowConstant>() : RowConstant();

    return constant ? canonicalArgument(constant.getRow()) : Value();
}

// The column of the rows @param value holds one value for: itself, or for a value computed per
// row, the first column it is computed over that is not a constant
Value rowsOf(Value value, Block& body) {
    Value rows = value;
    while (true) {
        Operation* const op = rows.getDefiningOp();
        if (!op || op->getBlock() != &body || !computesAValuePerRow(op)) {
            return rows;
        }

        const OperandRange operands = op->getOperands();
        const auto rowOperand = llvm::find_if(operands, [](Value operand) {
            return !::db::yieldsConstantColumn(operand);
        });
        if (rowOperand == operands.end()) {
            return rows;
        }

        rows = *rowOperand;
    }
}

// The argument whose run constant @param column holds on every row, mapped down from it by the
// ops of @param body; null where it holds none
Value heldRunConstantArgument(Value column, Block& body) {
    Value rows = column;
    while (rows) {
        if (const Value argument = runConstantArgument(rows)) {
            return argument;
        }

        const OpResult result = dyn_cast<OpResult>(rows);
        Operation* const op = result ? result.getOwner() : nullptr;

        const bool mapsRowsInTheBody = op && op->getBlock() == &body && mapsRowsThrough(op);
        if (!mapsRowsInTheBody) {
            return {};
        }

        rows = cast<ColumnCarrierOpInterface>(op).getRowSource(result);
    }

    return {};
}

Value runConstantOf(Block& body, Value argument) {
    for (const BlockArgument candidate : body.getArguments()) {
        if (canonicalArgument(candidate) != argument) {
            continue;
        }

        for (Operation* const user : candidate.getUsers()) {
            if (RowConstant constant = dyn_cast<RowConstant>(user)) {
                return constant.getResult();
            }
        }
    }

    return {};
}

// The rows @param body yields, as the first yielded column holding one value per row is over
Value yieldedRowsOf(Block& body) {
    const OperandRange yielded = yieldedColumnsOf(body);
    for (const Value column : yielded) {
        const Value rows = rowsOf(column, body);
        if (!::db::yieldsConstantColumn(rows)) {
            return rows;
        }
    }

    return {};
}

// The ops mapping rows through from the rows of @param yielded, a column @param body yields,
// down to the first with a result holding the run constant read off @param argument, which
// @param holder is set to; nearest the yield first
bool matchRunConstantChain(Block& body, Value yielded, Value argument, llvm::SmallVectorImpl<Operation*>& chain, Value& holder) {
    Value rows = rowsOf(yielded, body);
    while (rows) {
        const OpResult result = dyn_cast<OpResult>(rows);
        Operation* const op = result ? result.getOwner() : nullptr;

        const bool mapsRowsInTheBody = op && op->getBlock() == &body && mapsRowsThrough(op);
        if (!mapsRowsInTheBody) {
            return false;
        }

        const auto holding = llvm::find_if(op->getResults(), [&body, argument](OpResult other) {
            return heldRunConstantArgument(other, body) == argument;
        });

        if (holding != op->getResults().end()) {
            holder = *holding;
            return true;
        }

        rows = cast<ColumnCarrierOpInterface>(op).getRowSource(result);
        chain.push_back(op);
    }

    return false;
}

// The steps from the rows a body run per row yields down to a column holding the run constant
// its carried column at @param carriedIndex is read as, along the first yielded column they
// lead from. Where no column holds it, @param holder is the run constant itself, which is laid
// out over the yielded rows.
bool matchRunConstantChainOf(Block& body, size_t carriedIndex, llvm::SmallVectorImpl<Operation*>& chain, Value& holder) {
    const Value argument = canonicalArgument(carriedArgument(*body.getParent(), carriedIndex));

    const OperandRange yielded = yieldedColumnsOf(body);
    for (const Value column : yielded) {
        chain.clear();
        if (matchRunConstantChain(body, column, argument, chain, holder)) {
            return true;
        }
    }

    chain.clear();
    holder = runConstantOf(body, argument);

    return holder && yieldedRowsOf(body);
}

// A body run per row yields rows of one run, so the carried column a result hands back holds
// on each of them the run constant the body reads it as. The rows come out of the body from
// a column holding that constant, which the yielded rows map from.
bool matchRunConstantOutOfRegionStep(OpResult result, RowsStep& step) {
    RowsStep handsBack;
    if (!matchHandedBackResult(result, handsBack)) {
        return false;
    }

    Block& body = *handsBack._body;
    Region& region = *body.getParent();

    RegionCarrySet carrySet;
    matchRegionCarrySet(region, carrySet);

    size_t resultOffset = 0;
    ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(result.getOwner());
    const bool yieldsRowForRow = !carrySet._yieldsBack && carrier.getYieldedResultOffset(region, resultOffset);
    if (!yieldsRowForRow) {
        return false;
    }

    llvm::SmallVector<Operation*> chain;
    Value holder;
    if (!matchRunConstantChainOf(body, handsBack._position, chain, holder)) {
        return false;
    }

    step = RowsStep {._kind = RowsStepKind::RunConstantOutOfRegion,
                     ._op = chain.empty() ? body.getTerminator() : chain.back(),
                     ._body = &body,
                     ._position = handsBack._position,
                     ._rows = result,
                     ._outerRows = holder};

    return true;
}

// A non-optional CALL hands its carried columns back out of its body, so a result doing both
// steps out of the body: a read there reaches it, and a read before the op through the body
bool matchRowsStep(Value rows, RowsStep& step) {
    if (const BlockArgument argument = dyn_cast<BlockArgument>(rows)) {
        return matchIntoRegionStep(argument, step);
    }

    const OpResult result = cast<OpResult>(rows);

    return matchMapsRowsStep(result, step)
        || matchRowConstantStep(result, step)
        || matchOutOfRegionStep(result, step)
        || matchRunConstantOutOfRegionStep(result, step)
        || matchHandsBackStep(result, step);
}

// A walk into a body through the run constant a result hands back, and out of it through the
// argument that constant is read off, only passes the op: one step handing the column back
void collapseRunConstantRoundTrips(llvm::SmallVectorImpl<RowsStep>& steps) {
    for (size_t stepIndex = 0; stepIndex < steps.size(); stepIndex++) {
        if (steps[stepIndex]._kind != RowsStepKind::RunConstantOutOfRegion) {
            continue;
        }

        Block* const body = steps[stepIndex]._body;
        const auto exitIt = std::find_if(steps.begin() + stepIndex + 1, steps.end(), [body](const RowsStep& step) {
            return step._kind == RowsStepKind::IntoRegion && step._body == body;
        });
        if (exitIt == steps.end()) {
            continue;
        }

        RowsStep handsBack;
        const bool handsBackTheColumn = matchHandedBackResult(cast<OpResult>(steps[stepIndex]._rows), handsBack);
        bioassert(handsBackTheColumn, "A run constant comes out of a body as a column its op hands back");

        steps.erase(steps.begin() + stepIndex + 1, exitIt + 1);
        steps[stepIndex] = handsBack;
    }
}

// The op a column of the rows a step reaches has to stand before
Operation* outerUseSite(const RowsStep& step) {
    switch (step._kind) {
        case RowsStepKind::MapsRows:
        case RowsStepKind::ReadsRowConstant:
        case RowsStepKind::RunConstantOutOfRegion:
            return step._op;
        break;
        case RowsStepKind::IntoRegion:
        case RowsStepKind::HandsBack:
            return step._body->getParentOp();
        break;
        case RowsStepKind::OutOfRegion:
        case RowsStepKind::FromForwardingRegion:
            return step._body->getTerminator();
        break;
        case RowsStepKind::LaysOutConstant:
            return nullptr;
        break;
    }

    return nullptr;
}

// A run constant holds its one value for every row of its body, so a column read off it
// reaches the rows mapped from it without being carried, laid out over the rows the walk
// mapped down from
void layOutRunConstant(llvm::SmallVectorImpl<RowsStep>& steps) {
    size_t firstMapping = steps.size();
    while (firstMapping > 0 && steps[firstMapping - 1]._kind == RowsStepKind::MapsRows) {
        firstMapping--;
    }

    if (firstMapping == steps.size()) {
        return;
    }

    const Value entryRows = steps[firstMapping]._rows;
    steps.truncate(firstMapping);
    steps.push_back(RowsStep {._kind = RowsStepKind::LaysOutConstant, ._rows = entryRows});
}

// An op handing back the column it carries in at _carriedIndex for the rows of the one at
// _rowsIndex, and that column carried up _chain toward the body's yield, _nextStep steps of
// it still to go
struct HandBack {
    Operation* _op {nullptr};
    size_t _carriedIndex {0};
    size_t _rowsIndex {0};
    llvm::SmallVector<RowsStep> _chain;
    size_t _nextStep {0};
    Value _column;
};

// Hands a read the column holding what it reads of its rows: one read before it, or one read
// of rows its own come from, carried down to it through the ops between
class ReadColumnWalk {
public:
    ReadColumnWalk(KnownReadColumns& known, mlir::RewriterBase& rewriter)
        : _known(known),
          _rewriter(rewriter)
    {
    }

    // Null where there is none: this adds no read. A walk starts again once handing a column
    // back through a region has rebuilt ops a step of it stood on.
    Value columnOf(Operation* read) {
        _entityRead = entityReadOf(read);

        Value column;
        bool rebuilt = true;
        while (rebuilt) {
            llvm::SmallVector<RowsStep> steps;
            column = findKnownColumn(read, steps);
            if (!column) {
                return {};
            }

            rebuilt = false;
            for (size_t stepIndex = steps.size(); stepIndex > 0 && !rebuilt; stepIndex--) {
                column = carryBack(steps[stepIndex - 1], column, rebuilt);
            }
        }

        return column;
    }

private:
    KnownReadColumns& _known;
    mlir::RewriterBase& _rewriter;
    EntityRead _entityRead;

    Value findKnownColumn(Operation* read, llvm::SmallVectorImpl<RowsStep>& steps) {
        Value rows = read->getOperand(0);
        Operation* useSite = read;

        while (true) {
            if (isa_and_present<RowConstant>(rows.getDefiningOp())) {
                layOutRunConstant(steps);
            }

            const Value known = _known.find(rows, _entityRead, useSite);
            if (known) {
                collapseRunConstantRoundTrips(steps);
                return known;
            }

            RowsStep step;
            if (!matchRowsStep(rows, step)) {
                return {};
            }

            RowsStep forwarding;
            if (const Value forwarded = findInForwardingRegion(step, forwarding)) {
                steps.push_back(forwarding);
                collapseRunConstantRoundTrips(steps);
                return forwarded;
            }

            steps.push_back(step);
            rows = step._outerRows;
            useSite = outerUseSite(step);
        }
    }

    // The column a region before the one @param intoRegion steps into holds for the same
    // carried rows, nearest first, where that region forwards columns
    Value findInForwardingRegion(const RowsStep& intoRegion, RowsStep& forwarding) {
        if (intoRegion._kind != RowsStepKind::IntoRegion) {
            return {};
        }

        Region& region = *intoRegion._body->getParent();
        Operation* const op = region.getParentOp();
        ColumnCarrierOpInterface carrier = cast<ColumnCarrierOpInterface>(op);

        for (size_t regionIndex = region.getRegionNumber(); regionIndex > 0; regionIndex--) {
            Region& earlier = op->getRegion(static_cast<unsigned>(regionIndex - 1));
            if (!carrier.forwardsColumns(earlier)) {
                continue;
            }

            Block& earlierBody = earlier.front();
            const BlockArgument earlierRows = carriedArgument(earlier, intoRegion._position);

            const Value known = _known.find(earlierRows, _entityRead, earlierBody.getTerminator());
            if (known) {
                forwarding = RowsStep {._kind = RowsStepKind::FromForwardingRegion,
                                       ._body = &earlierBody,
                                       ._position = intoRegion._position,
                                       ._rows = intoRegion._rows,
                                       ._outerRows = earlierRows};
                return known;
            }
        }

        return {};
    }

    Value carryBack(const RowsStep& step, Value column, bool& rebuilt) {
        switch (step._kind) {
            case RowsStepKind::MapsRows:
                return carryThroughRows(step, column);
            break;
            case RowsStepKind::IntoRegion:
                return carryIntoBody(step, column, rebuilt);
            break;
            case RowsStepKind::FromForwardingRegion:
                return forwardIntoRegion(step, column);
            break;
            case RowsStepKind::ReadsRowConstant:
                return readAsRowConstant(step, column);
            break;
            case RowsStepKind::LaysOutConstant:
                return layOutConstant(step, column);
            break;
            case RowsStepKind::OutOfRegion:
                return carryOutOf(*step._body, column);
            break;
            case RowsStepKind::RunConstantOutOfRegion:
                return carryRunConstantOutOfBody(step, column);
            break;
            case RowsStepKind::HandsBack:
                return carryThroughBody(step, column);
            break;
        }

        return column;
    }

    Value carryThroughRows(const RowsStep& step, Value column) {
        const Value carried = carryThroughOp(step._op, column);
        Operation* const carrying = carried.getDefiningOp();

        _known.addCarried(carrying->getResult(static_cast<unsigned>(step._position)), _entityRead, carried);

        return carried;
    }

    Value carryThroughOp(Operation* carrier, Value column) {
        const llvm::SmallVector<Value> carrierResults(carrier->getResults());

        const Value carried = carryThrough(carrier, column, _rewriter);

        Operation* const carrying = carried.getDefiningOp();
        if (carrying != carrier) {
            llvm::SmallVector<Value> replacements;
            for (const Value result : carrying->getResults()) {
                if (result != carried) {
                    replacements.push_back(result);
                }
            }

            _known.rebind(carrierResults, replacements);
        }

        return carried;
    }

    Value carryIntoBody(const RowsStep& step, Value column, bool& rebuilt) {
        Block& body = *step._body;
        Operation* const op = body.getParentOp();

        bool appended = false;
        const size_t carriedIndex = carryIntoRegions(op, column, _rewriter, appended);

        RegionCarrySet carrySet;
        matchRegionCarrySet(*body.getParent(), carrySet);

        if (appended && carrySet._handsBack) {
            handBack(op, carriedIndex, step._position);
            rebuilt = carrySet._yieldsBack;
        }

        const BlockArgument argument = carriedArgument(*body.getParent(), carriedIndex);
        _known.addCarried(step._rows, _entityRead, argument);

        return argument;
    }

    Value forwardIntoRegion(const RowsStep& step, Value column) {
        Region& source = *step._body->getParent();
        Region& target = *cast<BlockArgument>(step._rows).getParentRegion();

        const BlockArgument argument = forwardColumn(source, column, target, _rewriter);
        _known.addCarried(step._rows, _entityRead, argument);

        return argument;
    }

    Value carryThroughBody(const RowsStep& step, Value column) {
        Operation* op = step._body->getParentOp();

        bool appended = false;
        const size_t carriedIndex = carryIntoRegions(op, column, _rewriter, appended);
        if (appended) {
            op = handBack(op, carriedIndex, step._position);
        }

        return op->getResult(static_cast<unsigned>(carriedIndex));
    }

    // @param op hands back the column it has just carried in at @param carriedIndex as a new
    // result. A body yielding its carried columns back yields it along the one at
    // @param rowsIndex, carried through each step between, and an op among them handing back
    // what it carries in hands this column back the same way before the steps above it go on.
    Operation* handBack(Operation* op, size_t carriedIndex, size_t rowsIndex) {
        llvm::SmallVector<HandBack> handBacks;
        pushHandBack(op, carriedIndex, rowsIndex, handBacks);

        while (true) {
            HandBack& current = handBacks.back();

            if (current._nextStep == 0) {
                const size_t handedIndex = current._carriedIndex;
                Operation* const rebuilt = finishHandBack(current);

                handBacks.pop_back();
                if (handBacks.empty()) {
                    return rebuilt;
                }

                handBacks.back()._column = rebuilt->getResult(static_cast<unsigned>(handedIndex));
                continue;
            }

            current._nextStep--;
            const RowsStep step = current._chain[current._nextStep];

            if (step._kind == RowsStepKind::MapsRows) {
                current._column = carryThroughOp(step._op, current._column);
                continue;
            }

            Operation* const handing = step._body->getParentOp();

            bool appended = false;
            const size_t handedIndex = carryIntoRegions(handing, current._column, _rewriter, appended);
            if (appended) {
                pushHandBack(handing, handedIndex, step._position, handBacks);
            } else {
                current._column = handing->getResult(static_cast<unsigned>(handedIndex));
            }
        }
    }

    void pushHandBack(Operation* op, size_t carriedIndex, size_t rowsIndex, llvm::SmallVectorImpl<HandBack>& handBacks) {
        Region& region = op->getRegion(0);

        RegionCarrySet carrySet;
        matchRegionCarrySet(region, carrySet);

        HandBack handBack {._op = op, ._carriedIndex = carriedIndex, ._rowsIndex = rowsIndex};
        if (carrySet._yieldsBack) {
            const bool chained = matchHandedBackChain(region.front(), rowsIndex, handBack._chain);
            bioassert(chained, "A column is carried into a body only along one it yields back");

            handBack._nextStep = handBack._chain.size();
            handBack._column = carriedArgument(region, carriedIndex);
        }

        handBacks.push_back(handBack);
    }

    Operation* finishHandBack(const HandBack& handBack) {
        Operation* const op = handBack._op;
        Region& region = op->getRegion(0);

        RegionCarrySet carrySet;
        matchRegionCarrySet(region, carrySet);

        if (carrySet._yieldsBack) {
            insertYieldedColumn(region.front(), handBack._carriedIndex, handBack._column);
        }

        const Value column = op->getOperand(static_cast<unsigned>(carrySet._operandOffset + handBack._carriedIndex));
        Operation* const rebuilt = insertResult(op, handBack._carriedIndex, column.getType());

        _known.addCarried(rebuilt->getResult(static_cast<unsigned>(handBack._rowsIndex)),
                          _entityRead,
                          rebuilt->getResult(static_cast<unsigned>(handBack._carriedIndex)));

        return rebuilt;
    }

    Value readAsRowConstant(const RowsStep& step, Value column) {
        _rewriter.setInsertionPointAfter(step._op);
        const Value constant = _rewriter.create<RowConstant>(step._op->getLoc(), column).getResult();

        _known.addCarried(step._rows, _entityRead, constant);

        return constant;
    }

    Value layOutConstant(const RowsStep& step, Value constant) {
        _rewriter.setInsertionPointAfterValue(step._rows);
        const Value laidOut = _rewriter.create<BroadcastConstant>(step._rows.getLoc(), constant, step._rows).getResult();

        _known.addCarried(step._rows, _entityRead, laidOut);

        return laidOut;
    }

    Value carryRunConstantOutOfBody(const RowsStep& step, Value column) {
        Block& body = *step._body;

        llvm::SmallVector<Operation*> chain;
        Value holder;
        const bool chained = matchRunConstantChainOf(body, step._position, chain, holder);
        bioassert(chained, "A run constant comes out of a body along the rows it yields");

        if (runConstantArgument(holder)) {
            Operation* const terminator = body.getTerminator();
            _rewriter.setInsertionPoint(terminator);

            const Value rows = yieldedRowsOf(body);
            const Value laidOut = _rewriter.create<BroadcastConstant>(terminator->getLoc(), column, rows).getResult();

            return carryOutOf(body, laidOut);
        }

        Value carried = column;
        for (size_t chainIndex = chain.size(); chainIndex > 0; chainIndex--) {
            carried = carryThroughOp(chain[chainIndex - 1], carried);
        }

        return carryOutOf(body, carried);
    }

    Value carryOutOf(Block& body, Value column) {
        Operation* op = body.getParentOp();

        size_t resultOffset = 0;
        const bool yields = cast<ColumnCarrierOpInterface>(op).getYieldedResultOffset(*body.getParent(), resultOffset);
        bioassert(yields, "A column comes out of a region whose yield the op's results stand for");

        const OperandRange yielded = yieldedColumnsOf(body);
        const auto yieldedIt = llvm::find(yielded, column);
        const size_t yieldIndex = static_cast<size_t>(yieldedIt - yielded.begin());

        if (yieldedIt == yielded.end()) {
            insertYieldedColumn(body, yieldIndex, column);
            op = insertResult(op, resultOffset + yieldIndex, column.getType());
        }

        return op->getResult(static_cast<unsigned>(resultOffset + yieldIndex));
    }

    void insertYieldedColumn(Block& body, size_t yieldIndex, Value column) {
        MutableOperandRange yielded = yieldedColumnsOf(body);
        const OperandRange current = yielded;

        llvm::SmallVector<Value> columns(current.begin(), current.end());
        columns.insert(columns.begin() + yieldIndex, column);

        _rewriter.modifyOpInPlace(body.getTerminator(), [&yielded, &columns] {
            yielded.assign(columns);
        });
    }

    // Rebuilds @param op with a result of @param type at @param resultIndex, the ones from there
    // on moving up one, and its regions moved over
    Operation* insertResult(Operation* op, size_t resultIndex, Type type) {
        llvm::SmallVector<Type> types(op->getResultTypes());
        types.insert(types.begin() + resultIndex, type);

        OperationState state(op->getLoc(), op->getName());
        state.addOperands(op->getOperands());
        state.addTypes(types);
        state.addAttributes(op->getAttrs());

        Operation* const rebuilt = rebuildOp(op, state, _rewriter);

        const llvm::SmallVector<Value> replaced(op->getResults());
        llvm::SmallVector<Value> replacements;
        for (size_t index = 0; index < replaced.size(); index++) {
            const size_t rebuiltIndex = index < resultIndex ? index : index + 1;
            replacements.push_back(rebuilt->getResult(static_cast<unsigned>(rebuiltIndex)));
        }

        _known.rebind(replaced, replacements);
        _rewriter.replaceOp(op, replacements);

        return rebuilt;
    }
};

void collectReads(Operation* root, llvm::function_ref<bool(Operation*)> isShared, llvm::SmallVectorImpl<Operation*>& reads) {
    root->walk([isShared, &reads](Operation* op) {
        if (isShared(op)) {
            reads.push_back(op);
        }
    });
}

using RegionReaderKey = std::tuple<Block*, Value, TypeID, Attribute>;

RegionReaderKey regionReaderKey(Block* block, Value rows, const EntityRead& entityRead) {
    return RegionReaderKey {block, canonicalRows(rows), entityRead._op, entityRead._property};
}

// The first op of a block whose regions read what a read takes off rows of that block, found
// along the steps the walk climbs from each read inside a region
void collectRegionReaders(llvm::ArrayRef<Operation*> reads, llvm::DenseMap<RegionReaderKey, Operation*>& readers) {
    llvm::DenseSet<std::tuple<Value, TypeID, Attribute>> climbed;
    for (Operation* const read : reads) {
        const EntityRead entityRead = entityReadOf(read);

        Value rows = read->getOperand(0);
        RowsStep step;
        while (matchRowsStep(rows, step)) {
            if (step._kind == RowsStepKind::IntoRegion) {
                Operation* const reader = step._body->getParentOp();
                const RegionReaderKey key = regionReaderKey(reader->getBlock(), step._outerRows, entityRead);

                const auto [readerIt, isFirst] = readers.try_emplace(key, reader);
                if (!isFirst && reader->isBeforeInBlock(readerIt->second)) {
                    readerIt->second = reader;
                }
            }

            rows = step._outerRows;
            if (!climbed.insert({rows, entityRead._op, entityRead._property}).second) {
                break;
            }
        }
    }
}

// A read runs over every row of its rows wherever it stands in their block, so one behind an
// op whose regions read the same moves in front of it, where the walk carries it in
void hoistReadsAboveRegionReaders(llvm::ArrayRef<Operation*> reads, mlir::RewriterBase& rewriter) {
    llvm::DenseMap<RegionReaderKey, Operation*> readers;
    collectRegionReaders(reads, readers);

    for (Operation* const read : reads) {
        const auto readerIt = readers.find(regionReaderKey(read->getBlock(), read->getOperand(0), entityReadOf(read)));
        if (readerIt != readers.end() && readerIt->second->isBeforeInBlock(read)) {
            rewriter.moveOpBefore(read, readerIt->second);
        }
    }
}

// Hands each read @param isShared picks the column a read before it, or one of rows its own
// come from, already holds, carried down to it
void shareReads(Operation* root, llvm::function_ref<bool(Operation*)> isShared, mlir::RewriterBase& rewriter) {
    llvm::SmallVector<Operation*> reads;
    collectReads(root, isShared, reads);
    hoistReadsAboveRegionReaders(reads, rewriter);

    reads.clear();
    collectReads(root, isShared, reads);

    KnownReadColumns knownColumns;
    ReadColumnWalk walk(knownColumns, rewriter);
    for (Operation* const read : reads) {
        const Value shared = walk.columnOf(read);
        if (!shared) {
            knownColumns.addRead(read);
            continue;
        }

        rewriter.replaceOp(read, shared);
    }
}

struct ReusePropertyReads : public impl::ReusePropertyReadsBase<ReusePropertyReads> {
    void runOnOperation() override {
        Operation* const root = getOperation();

        llvm::DenseSet<Attribute> writtenNodeProperties;
        llvm::DenseSet<Attribute> writtenEdgeProperties;
        bool writesEveryNodeProperty = false;
        bool writesEveryEdgeProperty = false;
        root->walk([&](Operation* op) {
            StringAttr writtenProperty;
            bool writesNodes = false;

            if (matchPropertyWrite(op, writtenProperty, writesNodes)) {
                if (writesNodes) {
                    writtenNodeProperties.insert(writtenProperty);
                } else {
                    writtenEdgeProperties.insert(writtenProperty);
                }
            } else if (matchPropertiesWrite(op, writesNodes)) {
                if (writesNodes) {
                    writesEveryNodeProperty = true;
                } else {
                    writesEveryEdgeProperty = true;
                }
            }
        });

        const auto isShared = [&](Operation* op) {
            StringAttr property;
            bool nodeProperty = false;
            if (!matchPropertyRead(op, property, nodeProperty)) {
                return false;
            }

            // A read standing before this one saw what the property held before the write,
            // and a projection behind a SET reads what the statement wrote
            const llvm::DenseSet<Attribute>& written = nodeProperty ? writtenNodeProperties : writtenEdgeProperties;
            const bool writesEveryProperty = nodeProperty ? writesEveryNodeProperty : writesEveryEdgeProperty;

            return !writesEveryProperty && !written.contains(property);
        };

        mlir::IRRewriter rewriter(&getContext());
        shareReads(root, isShared, rewriter);
    }
};

bool isLabelOrTypeRead(Operation* op) {
    return isa<GetNodeLabelSet, GetEdgeTypes>(op);
}

// Codegen reads the label sets of a node column, or the types of an edge column, once per
// test over it, and a predicate no check fusion folds keeps every one of those reads.
struct ReuseLabelAndTypeReads : public impl::ReuseLabelAndTypeReadsBase<ReuseLabelAndTypeReads> {
    void runOnOperation() override {
        llvm::SmallVector<Operation*> reads;
        collectReads(getOperation(), isLabelOrTypeRead, reads);

        mlir::IRRewriter rewriter(&getContext());
        llvm::DenseMap<std::tuple<Block*, Value, OperationName>, Operation*> firstReads;
        for (Operation* const read : reads) {
            if (const Value output = outputReadColumnOf(read->getOperand(0), entityReadOf(read))) {
                rewriter.replaceOp(read, output);
                continue;
            }

            const std::tuple<Block*, Value, OperationName> key {read->getBlock(), read->getOperand(0), read->getName()};
            const auto [entry, isFirst] = firstReads.try_emplace(key, read);
            if (!isFirst) {
                Operation* const firstRead = entry->second;
                rewriter.replaceOp(read, firstRead->getResults());
            }
        }
    }
};

struct CarryLabelAndTypeReads : public impl::CarryLabelAndTypeReadsBase<CarryLabelAndTypeReads> {
    void runOnOperation() override {
        mlir::IRRewriter rewriter(&getContext());
        shareReads(getOperation(), isLabelOrTypeRead, rewriter);
    }
};

class ConstantColumnCache : public mlir::RewriterBase::Listener {
public:
    bool yieldsConstantColumn(Value column) {
        return ::db::yieldsConstantColumn(column, _classified);
    }

    void notifyOperationErased(Operation* op) override {
        ::db::forgetErasedValues(op, _classified);
    }

private:
    llvm::DenseMap<Value, bool> _classified;
};

// The column in front of the op holding @param body that @param column holds row for row, and
// the ops of @param body mapping its rows down to @param column, nearest it first. Null for an
// element, an accumulator, a row tag and what the body computes.
Value outerColumnOf(Value column, Block& body, llvm::SmallVectorImpl<Operation*>& steps) {
    Value rows = column;
    while (rows) {
        if (const BlockArgument argument = dyn_cast<BlockArgument>(rows)) {
            return argument.getOwner() == &body ? carriedColumnOf(argument) : Value();
        }

        const OpResult result = cast<OpResult>(rows);
        Operation* const op = result.getOwner();
        const bool mapsTheBodyRows = op->getBlock() == &body && mapsRowsThrough(op);
        if (!mapsTheBodyRows) {
            return {};
        }

        steps.push_back(op);
        rows = cast<ColumnCarrierOpInterface>(op).getRowSource(result);
    }

    return {};
}

// The ops of @param body computing @param constants, in block order
void collectConstantCone(llvm::ArrayRef<Value> constants, Block& body, llvm::SmallVectorImpl<Operation*>& cone) {
    llvm::SmallPtrSet<Operation*, 8> inCone;
    llvm::SmallVector<Value> pending(constants.begin(), constants.end());

    while (!pending.empty()) {
        Operation* const op = pending.pop_back_val().getDefiningOp();
        const bool inBody = op && op->getBlock() == &body;
        const bool firstReached = inBody && inCone.insert(op).second;
        if (!firstReached) {
            continue;
        }

        cone.push_back(op);
        llvm::append_range(pending, op->getOperands());

        llvm::SetVector<Value> readAbove;
        mlir::getUsedValuesDefinedAbove(op->getRegions(), readAbove);
        llvm::append_range(pending, readAbove);
    }

    llvm::sort(cone, [](Operation* lhs, Operation* rhs) {
        return lhs->isBeforeInBlock(rhs);
    });
}

// An op of a region repeating rows that computes one value per row out of the columns carried
// in and constants alone. _operands are its operands in front of the op holding the region,
// _steps the ops of the region mapping the rows the columns come in as down to its own,
// nearest it first, and _constants the ops of the region computing the constants it reads.
struct RegionInvariantOp {
    llvm::SmallVector<Value> _operands;
    llvm::SmallVector<Operation*> _steps;
    llvm::SmallVector<Operation*> _constants;
};

// In front of the op holding its region, an op also runs on rows the region runs no time for,
// as a row whose list is empty. One that may raise or has an effect stays in the region, and so
// does one with a region of its own, whose cost per row has no bound: [y IN range(1, 50000) | i]
// would build 50000 elements for each row whose list is empty.
bool matchRegionInvariantOp(Operation* op, ConstantColumnCache& cache, RegionInvariantOp& invariant) {
    const bool computesOneValuePerRow = computesAValuePerRow(op) && op->getNumResults() == 1;
    const bool holdsNoRegion = op->getNumRegions() == 0;
    const bool isRead = !op->use_empty();
    if (!computesOneValuePerRow || !holdsNoRegion || !isRead) {
        return false;
    }

    Block& body = *op->getBlock();
    Region& region = *body.getParent();

    llvm::SmallVector<Value> constants;
    bool readsACarriedColumn = false;
    for (Value operand : op->getOperands()) {
        const bool definedAbove = !region.isAncestor(operand.getParentRegion());
        const bool readableInFront = definedAbove || cache.yieldsConstantColumn(operand);
        if (readableInFront) {
            invariant._operands.push_back(operand);
            constants.push_back(operand);
            continue;
        }

        llvm::SmallVector<Operation*> steps;
        const Value outer = outerColumnOf(operand, body, steps);
        const bool alignedWithTheOthers = !readsACarriedColumn || steps == invariant._steps;
        if (!outer || !alignedWithTheOthers) {
            return false;
        }

        invariant._operands.push_back(outer);
        invariant._steps = steps;
        readsACarriedColumn = true;
    }

    if (!readsACarriedColumn) {
        return false;
    }

    llvm::SetVector<Value> readAbove;
    mlir::getUsedValuesDefinedAbove(op->getRegions(), readAbove);
    for (Value value : readAbove) {
        const bool computedInTheRegion = region.isAncestor(value.getParentRegion()) && !cache.yieldsConstantColumn(value);
        if (computedInTheRegion) {
            return false;
        }

        constants.push_back(value);
    }

    const bool runsAnywhere = ::db::runsWithoutRaising(op) && ::db::hasNoEffectButRaising(op);
    if (!runsAnywhere) {
        return false;
    }

    collectConstantCone(constants, body, invariant._constants);

    return true;
}

void hoistRegionInvariantOp(Operation* op, const RegionInvariantOp& invariant, mlir::RewriterBase& rewriter) {
    Region& region = *op->getParentRegion();
    Operation* const holder = region.getParentOp();

    for (Operation* const constant : invariant._constants) {
        rewriter.moveOpBefore(constant, holder);
    }

    rewriter.moveOpBefore(op, holder);
    rewriter.modifyOpInPlace(op, [op, &invariant] {
        op->setOperands(invariant._operands);
    });

    const Value result = op->getResult(0);

    Value column = carryIntoRegion(region, result, rewriter);
    for (Operation* const step : llvm::reverse(invariant._steps)) {
        column = carryThrough(step, column, rewriter);
    }

    rewriter.replaceUsesWithIf(result, column, [&region](OpOperand& use) {
        return region.isAncestor(use.getOwner()->getParentRegion());
    });
}

// A hoist rewrites the ops reading the hoisted value, which come after it in the block, and
// rebuilds the steps it carries the value through, which come before it, so one sweep in
// block order meets every op once all the ops it reads are decided
void hoistRegionInvariantOps(Region& region, ConstantColumnCache& cache, mlir::RewriterBase& rewriter) {
    Block& body = region.front();

    Operation* op = &body.front();
    while (op) {
        Operation* const next = op->getNextNode();

        RegionInvariantOp invariant;
        if (matchRegionInvariantOp(op, cache, invariant)) {
            hoistRegionInvariantOp(op, invariant, rewriter);
        }

        op = next;
    }
}

struct HoistRepeatedRegionInvariantOps : public impl::HoistRepeatedRegionInvariantOpsBase<HoistRepeatedRegionInvariantOps> {
    void runOnOperation() override {
        llvm::SmallVector<Region*> repeatedRegions;
        getOperation()->walk([&repeatedRegions](Operation* op) {
            for (Region& region : op->getRegions()) {
                RegionCarrySet carrySet;
                const bool repeatsRows = !region.empty() && matchRegionCarrySet(region, carrySet) && carrySet._repeatsRows;
                if (repeatsRows) {
                    repeatedRegions.push_back(&region);
                }
            }
        });

        ConstantColumnCache cache;
        mlir::IRRewriter rewriter(&getContext(), &cache);
        for (Region* const region : repeatedRegions) {
            hoistRegionInvariantOps(*region, cache, rewriter);
        }
    }
};

}

}
