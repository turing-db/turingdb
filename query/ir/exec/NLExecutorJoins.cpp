#include "NLExecutor.h"

#include <stdint.h>
#include <algorithm>
#include <bit>
#include <concepts>
#include <functional>
#include <span>
#include <string_view>
#include <type_traits>

#include "NumberOrder.h"
#include "TypeUtils.h"
#include "ID.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "columns/ColumnOperator.h"
#include "columns/ColumnOperators.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/AllowedKinds.h"
#include "list/ListHash.h"
#include "map/MapHash.h"
#include "metadata/PropertyType.h"

#include "NLKeyHash.h"
#include "NLProgram.h"
#include "NLRuntimeProperty.h"
#include "NLColumnKernels.h"
#include "NLLoopSteps.h"

#include "IRException.h"
#include "BioAssert.h"

using namespace db;

namespace {

template <typename ElementType>
bool keyValuesEqual(const ElementType& probe, const ElementType& build) {
    return probe == build;
}

bool keyValuesEqual(types::Embedding::Primitive probe, types::Embedding::Primitive build) {
    return EmbeddingEqual {}(probe, build);
}

// == is equivalence on these, under which two nulls agree, so the join asks `=` itself
template <typename ElementType>
    requires std::same_as<ElementType, ListView>
          || std::same_as<ElementType, MapView>
          || std::same_as<ElementType, CellView>
bool keyValuesEqual(const ElementType& probe, const ElementType& build) {
    const std::optional<CustomBool> equal = Eq {}(probe, build);

    return equal.has_value() && equal->_boolean;
}

// A key no other key equals is one that does not equal itself: a null, a NaN, a list or
// map holding a null at any depth
template <typename ElementType>
bool keyValueIsMatchable(const ElementType& value) {
    return keyValuesEqual(value, value);
}

template <typename ElementType>
bool columnRowMatchable(const Column* column, size_t row) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();

    return keyValueIsMatchable(raw[row]);
}

template <typename Primitive>
bool optColumnRowMatchable(const Column* column, size_t row) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    return typedColumn->isValid(row) && keyValueIsMatchable(typedColumn->getValue(row));
}

template <typename Integral>
    requires std::is_integral_v<Integral> || std::is_enum_v<Integral>
uint64_t hashKeyValue(Integral value) {
    return mixKeyBits(static_cast<uint64_t>(value));
}

template <IntegralType T, int I>
uint64_t hashKeyValue(ID<T, I> id) {
    return hashKeyValue(id.getValue());
}

uint64_t hashKeyValue(CustomBool value) {
    return mixKeyBits(value._boolean);
}

uint64_t hashKeyValue(types::DateTime::Primitive value) {
    return mixKeyBits(static_cast<uint64_t>(value.getMicroseconds()));
}

uint64_t hashKeyValue(types::Duration::Primitive value) {
    return mixKeyBits(static_cast<uint64_t>(value.getMicroseconds()));
}

uint64_t hashKeyValue(types::Point::Primitive value) {
    return mixKeyBits(std::hash<types::Point::Primitive> {}(value));
}

// The two keys of a join need not share a type, so two values `=` holds equal hash alike
// whatever they are held as: a double holding a whole number hashes as that integer. -0.0
// hashes as +0.0, the one value it equals; a NaN never reaches a lookup.
uint64_t hashKeyValue(double value) {
    types::Int64::Primitive integer = 0;
    types::UInt64::Primitive unsignedInteger = 0;

    if (convertNumberExactly(value, integer)) {
        return hashKeyValue(integer);
    } else if (convertNumberExactly(value, unsignedInteger)) {
        return hashKeyValue(unsignedInteger);
    }

    return mixKeyBits(std::bit_cast<uint64_t>(value));
}

uint64_t hashKeyValue(std::string_view value) {
    return std::hash<std::string_view> {}(value);
}

uint64_t hashKeyValue(MapView map) {
    return mixKeyBits(hashMap(map));
}

uint64_t hashKeyValue(types::Embedding::Primitive embedding) {
    uint64_t hash = mixKeyBits(embedding.size());

    for (const float value : embedding) {
        const float normalized = (value == 0.0f) ? 0.0f : value;
        hash = mixKeyBits(hash ^ std::bit_cast<uint32_t>(normalized));
    }

    return hash;
}

uint64_t hashKeyValue(ListView list) {
    return mixKeyBits(hashList(list));
}

uint64_t hashKeyValue(PathView path) {
    const std::span<const uint64_t> ids = path.getIDs();
    uint64_t hash = mixKeyBits(ids.size());

    for (const uint64_t id : ids) {
        hash = mixKeyBits(hash ^ id);
    }

    return hash;
}

// A tagged cell hashes as its value does in a column of that value's type
uint64_t hashKeyValue(CellView cell) {
    switch (cell.getTag()) {
        case CellTag::Int:
            return hashKeyValue(cell.getAs<types::Int64::Primitive>());
        break;

        case CellTag::UInt:
            return hashKeyValue(cell.getAs<types::UInt64::Primitive>());
        break;

        case CellTag::Double:
            return hashKeyValue(cell.getAs<types::Double::Primitive>());
        break;

        case CellTag::Bool:
            return hashKeyValue(cell.getAs<types::Bool::Primitive>());
        break;

        case CellTag::String:
            return hashKeyValue(cell.getAs<types::String::Primitive>());
        break;

        case CellTag::Embedding:
            return hashKeyValue(cell.getAs<types::Embedding::Primitive>());
        break;

        case CellTag::ListView:
            return hashKeyValue(cell.getAs<ListView>());
        break;

        case CellTag::MapView:
            return hashKeyValue(cell.getAs<MapView>());
        break;

        case CellTag::NodeID:
            return hashKeyValue(cell.getAs<NodeID>());
        break;

        case CellTag::EdgeID:
            return hashKeyValue(cell.getAs<EdgeID>());
        break;

        case CellTag::DateTime:
            return hashKeyValue(cell.getAs<types::DateTime::Primitive>());
        break;

        case CellTag::Duration:
            return hashKeyValue(cell.getAs<types::Duration::Primitive>());
        break;

        case CellTag::Point:
            return hashKeyValue(cell.getAs<types::Point::Primitive>());
        break;

        case CellTag::Path:
            return hashKeyValue(cell.getAs<PathView>());
        break;

        case CellTag::LabelID:
            return hashKeyValue(cell.getAs<LabelID>());
        break;

        case CellTag::EdgeTypeID:
            return hashKeyValue(cell.getAs<EdgeTypeID>());
        break;

        case CellTag::PropertyTypeID:
            return hashKeyValue(cell.getAs<PropertyTypeID>());
        break;

        case CellTag::ValueType:
            return hashKeyValue(cell.getAs<ValueType>());
        break;

        case CellTag::Null:
        case CellTag::INVALID:
            return 0;
        break;
    }

    bioassert(false, "Unknown CellTag");
    return 0;
}

template <typename ElementType>
void hashKeyColumn(const Column* column, std::vector<uint64_t>& hashes) {
    const auto& raw = static_cast<const ColumnVector<ElementType>*>(column)->getRaw();
    hashes.resize(raw.size());

    std::transform(raw.begin(), raw.end(), hashes.begin(), [](const ElementType& value) {
        return hashKeyValue(value);
    });
}

template <typename Primitive>
void hashOptKeyColumn(const Column* column, std::vector<uint64_t>& hashes) {
    const ColumnOptVector<Primitive>* typedColumn = static_cast<const ColumnOptVector<Primitive>*>(column);

    const std::vector<Primitive>& values = typedColumn->getValues();
    const size_t rowCount = values.size();
    hashes.resize(rowCount);

    if (typedColumn->getNullCount() != 0) {
        std::fill(hashes.begin(), hashes.end(), uint64_t {0});
    }

    typedColumn->forEachPresentRun(0, rowCount, [&](size_t first, size_t last) {
        std::transform(values.begin() + first, values.begin() + last, hashes.begin() + first, [](const Primitive& value) {
            return hashKeyValue(value);
        });
    });
}

template <typename ElementType>
bool keyEqualColumn(const Column* probe, size_t probeRow, const Column* build, size_t buildRow) {
    const auto& probeRaw = static_cast<const ColumnVector<ElementType>*>(probe)->getRaw();
    const auto& buildRaw = static_cast<const ColumnVector<ElementType>*>(build)->getRaw();

    return keyValuesEqual(probeRaw[probeRow], buildRaw[buildRow]);
}

template <typename Primitive>
bool keyEqualOptColumn(const Column* probe, size_t probeRow, const Column* build, size_t buildRow) {
    const ColumnOptVector<Primitive>* typedProbe = static_cast<const ColumnOptVector<Primitive>*>(probe);
    const ColumnOptVector<Primitive>* typedBuild = static_cast<const ColumnOptVector<Primitive>*>(build);

    return keyValuesEqual(typedProbe->getValue(probeRow), typedBuild->getValue(buildRow));
}

template <typename ElementType>
NLJoinKeyFunctions joinKeyFunctions() {
    return {&hashKeyColumn<ElementType>, &keyEqualColumn<ElementType>, &columnRowMatchable<ElementType>};
}

template <typename Primitive>
NLJoinKeyFunctions optJoinKeyFunctions() {
    return {&hashOptKeyColumn<Primitive>, &keyEqualOptColumn<Primitive>, &optColumnRowMatchable<Primitive>};
}

template <typename ColumnType>
concept RowKeyColumn = !IS_CONSTANT_COLUMN<ColumnType> && requires(const ColumnType* column) { operandRow(column, size_t {0}); };

// A probe key and a build key of two types, compared by the `=` of the filter the join
// replaces: an unknown answer matches nothing, as the filter kept only the true rows
template <typename ProbeColumn, typename BuildColumn>
bool keyEqualAcrossTypes(const Column* probe, size_t probeRow, const Column* build, size_t buildRow) {
    const ProbeColumn* typedProbe = static_cast<const ProbeColumn*>(probe);
    const BuildColumn* typedBuild = static_cast<const BuildColumn*>(build);

    const auto equal = Eq {}(operandRow(typedProbe, probeRow), operandRow(typedBuild, buildRow));
    if constexpr (TypeUtils::is_optional_v<std::decay_t<decltype(equal)>>) {
        return equal.has_value() && equal->_boolean;
    } else {
        return static_cast<bool>(equal);
    }
}

struct CrossTypeKeyEqualSelector {
    NLKeyEqualFunction _equal {nullptr};

    template <typename ProbeColumn, typename BuildColumn>
    void operator()(const ProbeColumn*, const BuildColumn*) {
        if constexpr (RowKeyColumn<ProbeColumn> && RowKeyColumn<BuildColumn>) {
            _equal = &keyEqualAcrossTypes<ProbeColumn, BuildColumn>;
        }
    }
};

// The group of build rows carrying a key, or noRow when no indexed row carries it. One
// bucket chains the groups of every key that falls in it, so a group's key is confirmed
// by hash and then by value.
size_t findKeyGroup(const NLHashJoinIndex& index,
                    uint64_t hash,
                    const Column* probeKey,
                    size_t probeRow,
                    const Column* buildKey,
                    NLKeyEqualFunction keyEqual) {
    for (size_t group = index.getFirstGroup(hash); group != NLHashJoinIndex::noRow; group = index.getNextGroup(group)) {
        const bool sameHash = index.getHash(group) == hash;

        if (sameHash && keyEqual(probeKey, probeRow, buildKey, group)) {
            return group;
        }
    }

    return NLHashJoinIndex::noRow;
}

}

void NLExecutor::runCrossProductLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLCrossProductLoopData* loopData = static_cast<NLCrossProductLoopData*>(data);

    const NLCrossProductLoopData::Columns& outerColumns = loopData->outerColumns();
    const NLCrossProductLoopData::Columns& innerColumns = loopData->innerColumns();
    bioassert(!outerColumns.empty() && !innerColumns.empty(),
              "nl.cross_product needs a column on each side to size the product");

    // N outer rows crossed with M inner rows make N*M pairs. The counts come from
    // the first column of each side; every column of a side is row-aligned with
    // that first one, which is what lets one position slice them all.
    const size_t outerRowCount = outerColumns.front().getInput()->size();
    const size_t innerRowCount = innerColumns.front().getInput()->size();

    const size_t productRowCount = outerRowCount * innerRowCount;
    const size_t chunkSize = context->getChunkSize();
    const NLLimitState* limit = loopData->getLimit();
    const NLStmtContainer* loopBody = loopData->getStmts();

    // Walk the pairs a chunk at a time. Under a limit the step is cut to what the
    // budget can still emit - the pairs come out in (outer, inner) order, so the
    // step's prefix is exactly what the nl.limit_update/nl.output below can take,
    // and the loop stops once the budget is spent, as a scan's loop does.
    size_t position = 0;
    while (position < productRowCount) {
        const size_t remaining = limit ? limit->getRemaining() : productRowCount;
        if (remaining == 0) {
            return;
        }

        const size_t chunkRowCount = std::min(chunkSize, productRowCount - position);
        const size_t rowCount = std::min(chunkRowCount, remaining);

        for (const NLCrossColumn& column : outerColumns) {
            const NLBroadcastFunction broadcast = column.getBroadcast();
            broadcast(column.getInput(), innerRowCount, position, rowCount, column.getOutput());
        }

        for (const NLCrossColumn& column : innerColumns) {
            const NLBroadcastFunction broadcast = column.getBroadcast();
            broadcast(column.getInput(), innerRowCount, position, rowCount, column.getOutput());
        }

        runBody(context, loopBody);

        position += rowCount;
    }
}

void NLExecutor::runHashJoinReset(NLExecutionContext* context, NLFunctionData* data) {
    const NLHashJoinResetData* reset = static_cast<NLHashJoinResetData*>(data);
    reset->getState()->reset();
}

void NLExecutor::runHashJoinCollect(NLExecutionContext* context, NLFunctionData* data) {
    NLHashJoinCollectData* collect = static_cast<NLHashJoinCollectData*>(data);
    NLHashJoinState* state = collect->getState();
    NLHashJoinIndex& index = state->getIndex();

    const Column* key = collect->getKeyColumn();
    const size_t rowCount = key->size();
    const size_t firstRow = index.getRowCount();

    const NLKeyHashFunction hashKeys = collect->getKeyHash();
    std::vector<uint64_t>& hashes = *collect->getHashScratch();
    hashKeys(key, hashes);

    // Appending before indexing puts every row a key could join with in the buffer, so a
    // new row's key is compared against one already indexed through that buffer alone.
    for (const NLSortCollectData::Append& append : collect->appends()) {
        appendCollectedChunk(append, state->values());
    }

    index.addRows(rowCount);

    const Column* buildKey = collect->getBuildKeyColumn();
    const NLKeyEqualFunction keyEqual = collect->getKeyEqual();
    const NLKeyIsMatchableFunction keyIsMatchable = collect->getKeyIsMatchable();

    // A key no probe key can match - a null, a NaN - is indexed under nothing, so no probe
    // row reaches it.
    for (size_t row = 0; row < rowCount; row++) {
        if (!keyIsMatchable(key, row)) {
            continue;
        }

        const uint64_t hash = hashes[row];
        const size_t bufferRow = firstRow + row;
        const size_t group = findKeyGroup(index, hash, buildKey, bufferRow, buildKey, keyEqual);

        if (group == NLHashJoinIndex::noRow) {
            index.openGroup(bufferRow, hash);
        } else {
            index.addToGroup(group, bufferRow);
        }
    }
}

void NLExecutor::runHashJoinProbeLoop(NLExecutionContext* context, NLFunctionData* data) {
    NLHashJoinProbeLoopData* probe = static_cast<NLHashJoinProbeLoopData*>(data);
    const NLHashJoinState* state = probe->getState();
    const NLHashJoinIndex& index = state->getIndex();

    const Column* key = probe->getKeyColumn();
    const Column* buildKey = probe->getBuildKeyColumn();
    const size_t rowCount = key->size();

    const NLKeyHashFunction hashKeys = probe->getKeyHash();
    const NLKeyEqualFunction keyEqual = probe->getKeyEqual();
    const NLKeyIsMatchableFunction keyIsMatchable = probe->getKeyIsMatchable();

    std::vector<uint64_t>& hashes = *probe->getHashScratch();
    hashKeys(key, hashes);

    ColumnVector<size_t>* probeIndices = probe->getProbeIndices();
    ColumnVector<size_t>* buildIndices = probe->getBuildIndices();
    std::vector<size_t>& probeRaw = probeIndices->getRaw();
    std::vector<size_t>& buildRaw = buildIndices->getRaw();

    const size_t chunkSize = context->getChunkSize();
    const NLLimitState* limit = probe->getLimit();
    const NLStmtContainer* loopBody = probe->getStmts();

    // Walk the pairs a chunk at a time, holding the probe row reached and the next row of
    // its group to pair, so a step can start and end partway through one row's matches.
    // noRow with nothing left to pair is what advances the probe row.
    size_t probeRow = 0;
    size_t match = NLHashJoinIndex::noRow;
    bool inGroup = false;

    while (probeRow < rowCount) {
        const size_t remaining = limit ? limit->getRemaining() : chunkSize;
        if (remaining == 0) {
            return;
        }

        const size_t stepBudget = std::min(chunkSize, remaining);

        probeRaw.clear();
        buildRaw.clear();

        while (probeRow < rowCount && probeRaw.size() < stepBudget) {
            if (!inGroup) {
                if (!keyIsMatchable(key, probeRow)) {
                    probeRow++;
                    continue;
                }

                match = findKeyGroup(index, hashes[probeRow], key, probeRow, buildKey, keyEqual);
                inGroup = true;
            }

            // Every row of the group carries the key this probe row matched, so the group
            // is walked without comparing again.
            while (match != NLHashJoinIndex::noRow && probeRaw.size() < stepBudget) {
                probeRaw.push_back(probeRow);
                buildRaw.push_back(match);

                match = index.getNextInGroup(match);
            }

            if (match == NLHashJoinIndex::noRow) {
                probeRow++;
                inGroup = false;
            }
        }

        if (probeRaw.empty()) {
            return;
        }

        for (const NLCarriedColumn& column : probe->probeColumns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), probeIndices, column.getOutput());
        }

        for (const NLCarriedColumn& column : probe->buildColumns()) {
            const NLGatherFunction gather = column.getGatherFunc();
            gather(column.getInput(), buildIndices, column.getOutput());
        }

        runBody(context, loopBody);
    }
}

NLJoinKeyFunctions NLExecutor::selectJoinKeyFunctions(NLChunkKind kind) {
    switch (kind) {
        case NLChunkKind::NodeID:
            return joinKeyFunctions<NodeID>();
        break;

        case NLChunkKind::EdgeID:
            return joinKeyFunctions<EdgeID>();
        break;

        case NLChunkKind::EdgeTypeID:
            return joinKeyFunctions<EdgeTypeID>();
        break;

        case NLChunkKind::LabelSetID:
            return joinKeyFunctions<LabelSetID>();
        break;

        case NLChunkKind::LabelID:
            return joinKeyFunctions<LabelID>();
        break;

        case NLChunkKind::PropertyTypeID:
            return joinKeyFunctions<PropertyTypeID>();
        break;

        case NLChunkKind::ValueTypeCode:
            return joinKeyFunctions<ValueType>();
        break;

        case NLChunkKind::UInt64:
            return joinKeyFunctions<types::UInt64::Primitive>();
        break;

        case NLChunkKind::Int64:
            return joinKeyFunctions<types::Int64::Primitive>();
        break;

        case NLChunkKind::Double:
            return joinKeyFunctions<types::Double::Primitive>();
        break;

        case NLChunkKind::Bool:
            return joinKeyFunctions<types::Bool::Primitive>();
        break;

        case NLChunkKind::String:
            return joinKeyFunctions<types::String::Primitive>();
        break;

        case NLChunkKind::Embedding:
            return joinKeyFunctions<types::Embedding::Primitive>();
        break;

        case NLChunkKind::List:
            return joinKeyFunctions<ListView>();
        break;

        case NLChunkKind::Map:
            return joinKeyFunctions<MapView>();
        break;

        case NLChunkKind::Path:
            throw IRException("A path column cannot be a join key: a path has no scalar value to key on");
        break;

        case NLChunkKind::DateTime:
            return joinKeyFunctions<types::DateTime::Primitive>();
        break;

        case NLChunkKind::Duration:
            return joinKeyFunctions<types::Duration::Primitive>();
        break;

        case NLChunkKind::Point:
            return joinKeyFunctions<types::Point::Primitive>();
        break;

        case NLChunkKind::PathRef:
            throw IRException("A path column cannot be a join key: expand it into its list first");
        break;
    }

    bioassert(false, "Unknown NLChunkKind");
    return {};
}

NLJoinKeyFunctions NLExecutor::selectOptJoinKeyFunctions(NLChunkKind kind) {
    NLJoinKeyFunctions functions;
    dispatchOptChunkKind(kind, [&]<typename Element>() { functions = optJoinKeyFunctions<Element>(); });

    return functions;
}

NLJoinKeyFunctions NLExecutor::selectCellJoinKeyFunctions() {
    return joinKeyFunctions<CellView>();
}

// The pairs `=` compares are the pairs its kernel takes, so a pair the kernel has no
// comparison for never reaches a join: the filter it replaces could not have run.
NLKeyEqualFunction NLExecutor::selectCrossTypeKeyEqualFunction(const Column* probe, const Column* build) {
    using Pairs = PairRestrictions<OP_EQUAL>;

    CrossTypeKeyEqualSelector selector;
    ColumnDoubleDispatcher<typename Pairs::Allowed,
                           CrossTypeKeyEqualSelector,
                           typename Pairs::Excluded>::dispatch(probe, build, selector);

    if (!selector._equal) {
        throw IRException("A hash join key must be a column holding one value per row");
    }

    return selector._equal;
}
