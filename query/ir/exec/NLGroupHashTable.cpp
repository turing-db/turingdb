#include "NLGroupHashTable.h"

#include <string.h>

#include <algorithm>
#include <concepts>
#include <functional>

#include "NLKeyHash.h"

using namespace db;

namespace {

constexpr size_t INITIAL_CELL_COUNT = 256;

constexpr size_t ASSIGN_BATCH_ROWS = 256;

// Past this size most lookups miss the cache, so a lookup prefetches the cell of the row
// PREFETCH_DISTANCE rows further on
constexpr size_t PREFETCH_TABLE_BYTES = 1 << 20;
constexpr size_t PREFETCH_DISTANCE = 16;

template <size_t Words>
uint64_t hashPackedKey(const NLPackedKey<Words>& key) {
    uint64_t hash = mixKeyBits(key._words[0]);

    for (size_t word = 1; word < Words; word++) {
        hash = mixKeyBits(hash ^ key._words[word]);
    }

    return hash;
}

void packRowGroupColumn(const Column* column, size_t beginRow, size_t endRow, const NLKeyPackTarget& target) {
    const std::vector<size_t>& raw = static_cast<const ColumnVector<size_t>*>(column)->getRaw();
    std::byte* value = target._values;

    for (size_t row = beginRow; row < endRow; row++) {
        memcpy(value, &raw[row], sizeof(size_t));
        value += target._valueStride;
    }
}

void appendRowGroupKey(const Column* column, size_t row, NLSerializedKey& key) {
    const std::vector<size_t>& raw = static_cast<const ColumnVector<size_t>*>(column)->getRaw();
    key.append(reinterpret_cast<const char*>(&raw[row]), sizeof(size_t));
}

// Re-place every filled cell of a table, which is kept at most half full, into one twice its size
template <typename Cell, typename CellHash>
void growCells(std::vector<Cell>& cells, CellHash cellHash) {
    std::vector<Cell> grown(cells.size() * 2);
    const size_t mask = grown.size() - 1;

    for (const Cell& cell : cells) {
        if (cell._group == NLGroupAssignment::NO_GROUP) {
            continue;
        }

        size_t slot = cellHash(cell) & mask;
        while (grown[slot]._group != NLGroupAssignment::NO_GROUP) {
            slot = (slot + 1) & mask;
        }

        grown[slot] = cell;
    }

    cells.swap(grown);
}

}

NLGroupAssignment::NLGroupAssignment(std::vector<size_t>& groups, std::vector<size_t>& newGroupRows, size_t& groupCount)
    : _groups(groups),
    _newGroupRows(newGroupRows),
    _groupCount(groupCount)
{
}

NLGroupAssignment::~NLGroupAssignment() {
}

size_t NLGroupAssignment::createGroup(size_t row) {
    _newGroupRows.push_back(row);

    return _groupCount++;
}

void NLGroupAssignment::assignNullGroup(size_t row, size_t* nullGroup) {
    if (*nullGroup == NO_GROUP) {
        *nullGroup = createGroup(row);
    }

    setGroup(row, *nullGroup);
}

void NLKeylessGroupHashTable::assign(std::span<const Column* const> keyColumns, NLGroupAssignment& assignment) {
    const size_t rowCount = assignment.getRowCount();

    for (size_t row = 0; row < rowCount; row++) {
        assignment.setGroup(row, 0);
    }
}

void NLKeylessGroupHashTable::clear() {
}

template <typename Key, bool NullAside>
NLPackedKeyBatch<Key, NullAside>::NLPackedKeyBatch() {
}

template <typename Key, bool NullAside>
NLPackedKeyBatch<Key, NullAside>::~NLPackedKeyBatch() {
}

template <typename Key, bool NullAside>
void NLPackedKeyBatch<Key, NullAside>::pack(std::span<const Column* const> keyColumns, size_t beginRow, size_t endRow) {
    const size_t batchRows = endRow - beginRow;

    _keys.assign(batchRows, Key {});

    std::byte* keyBytes = reinterpret_cast<std::byte*>(_keys.data());

    NLKeyPackTarget target;
    target._valueStride = sizeof(Key);

    if constexpr (NullAside) {
        _nulls.assign(batchRows, std::byte {0});
        target._nulls = _nulls.data();
        target._nullStride = 1;
    } else {
        target._nulls = keyBytes;
        target._nullStride = sizeof(Key);
    }

    for (size_t keyIndex = 0; keyIndex < _keyColumns.size(); keyIndex++) {
        const NLPackedKeyColumn& column = _keyColumns[keyIndex];

        target._values = keyBytes + column._valueOffset;
        target._nullBit = column._nullBit;

        column._pack(keyColumns[keyIndex], beginRow, endRow, target);
    }
}

template <typename Key, bool NullAside>
NLDirectGroupHashTable<Key, NullAside>::NLDirectGroupHashTable() {
}

template <typename Key, bool NullAside>
NLDirectGroupHashTable<Key, NullAside>::~NLDirectGroupHashTable() {
}

template <typename Key, bool NullAside>
NLGroupHashTableKind NLDirectGroupHashTable<Key, NullAside>::getKind() const {
    if constexpr (std::same_as<Key, uint8_t> && NullAside) {
        return NLGroupHashTableKind::NullableDirect8;
    } else if constexpr (std::same_as<Key, uint8_t>) {
        return NLGroupHashTableKind::Direct8;
    } else if constexpr (NullAside) {
        return NLGroupHashTableKind::NullableDirect16;
    } else {
        return NLGroupHashTableKind::Direct16;
    }
}

template <typename Key, bool NullAside>
void NLDirectGroupHashTable<Key, NullAside>::assign(std::span<const Column* const> keyColumns,
                                                    NLGroupAssignment& assignment) {
    const size_t rowCount = assignment.getRowCount();

    if (_groups.empty()) {
        _groups.assign(std::numeric_limits<Key>::max() + size_t {1}, NLGroupAssignment::NO_GROUP);
    }

    for (size_t beginRow = 0; beginRow < rowCount; beginRow += ASSIGN_BATCH_ROWS) {
        const size_t endRow = std::min(rowCount, beginRow + ASSIGN_BATCH_ROWS);

        _batch.pack(keyColumns, beginRow, endRow);
        const std::vector<Key>& keys = _batch.getKeys();

        for (size_t index = 0; index < keys.size(); index++) {
            const size_t row = beginRow + index;

            if constexpr (NullAside) {
                if (_batch.isNull(index)) {
                    assignment.assignNullGroup(row, &_nullGroup);
                    continue;
                }
            }

            const Key key = keys[index];
            size_t& group = _groups[key];

            if (group == NLGroupAssignment::NO_GROUP) {
                group = assignment.createGroup(row);
                _groupKeys.push_back(key);
            }

            assignment.setGroup(row, group);
        }
    }
}

// A grouping in a subquery run once per row clears its table once per row, so only the slots
// holding a group are reset, not the whole array
template <typename Key, bool NullAside>
void NLDirectGroupHashTable<Key, NullAside>::clear() {
    for (const Key key : _groupKeys) {
        _groups[key] = NLGroupAssignment::NO_GROUP;
    }

    _groupKeys.clear();
    _nullGroup = NLGroupAssignment::NO_GROUP;
}

template <size_t Words, bool NullAside>
NLPackedGroupHashTable<Words, NullAside>::NLPackedGroupHashTable()
    : _cells(INITIAL_CELL_COUNT)
{
}

template <size_t Words, bool NullAside>
NLPackedGroupHashTable<Words, NullAside>::~NLPackedGroupHashTable() {
}

template <size_t Words, bool NullAside>
NLGroupHashTableKind NLPackedGroupHashTable<Words, NullAside>::getKind() const {
    if constexpr (NullAside) {
        return NLGroupHashTableKind::NullableFixed64;
    } else if constexpr (Words == 1) {
        return NLGroupHashTableKind::Fixed64;
    } else if constexpr (Words == 2) {
        return NLGroupHashTableKind::Fixed128;
    } else {
        return NLGroupHashTableKind::Fixed256;
    }
}

template <size_t Words, bool NullAside>
void NLPackedGroupHashTable<Words, NullAside>::assign(std::span<const Column* const> keyColumns,
                                                      NLGroupAssignment& assignment) {
    const size_t rowCount = assignment.getRowCount();

    for (size_t beginRow = 0; beginRow < rowCount; beginRow += ASSIGN_BATCH_ROWS) {
        const size_t endRow = std::min(rowCount, beginRow + ASSIGN_BATCH_ROWS);
        const size_t batchRows = endRow - beginRow;

        _batch.pack(keyColumns, beginRow, endRow);
        const std::vector<Key>& keys = _batch.getKeys();

        _batchHashes.resize(batchRows);
        std::transform(keys.begin(), keys.end(), _batchHashes.begin(), &hashPackedKey<Words>);

        const bool prefetches = _cells.size() * sizeof(Cell) > PREFETCH_TABLE_BYTES;

        for (size_t index = 0; index < batchRows; index++) {
            const size_t row = beginRow + index;

            if (prefetches && index + PREFETCH_DISTANCE < batchRows) {
                const size_t aheadSlot = _batchHashes[index + PREFETCH_DISTANCE] & (_cells.size() - 1);
                __builtin_prefetch(&_cells[aheadSlot]);
            }

            if constexpr (NullAside) {
                if (_batch.isNull(index)) {
                    assignment.assignNullGroup(row, &_nullGroup);
                    continue;
                }
            }

            const Key& key = keys[index];
            const bool continuesTheRun = (_lastGroup != NLGroupAssignment::NO_GROUP) && (key == _lastKey);

            if (!continuesTheRun) {
                _lastKey = key;
                _lastGroup = findOrCreateGroup(key, _batchHashes[index], row, assignment);
            }

            assignment.setGroup(row, _lastGroup);
        }
    }
}

template <size_t Words, bool NullAside>
void NLPackedGroupHashTable<Words, NullAside>::clear() {
    _cells.assign(INITIAL_CELL_COUNT, Cell {});
    _filledCells = 0;
    _nullGroup = NLGroupAssignment::NO_GROUP;
    _lastGroup = NLGroupAssignment::NO_GROUP;
}

template <size_t Words, bool NullAside>
size_t NLPackedGroupHashTable<Words, NullAside>::findOrCreateGroup(const Key& key,
                                                                   uint64_t hash,
                                                                   size_t row,
                                                                   NLGroupAssignment& assignment) {
    const size_t mask = _cells.size() - 1;
    size_t slot = hash & mask;

    while (true) {
        const Cell& cell = _cells[slot];

        if (cell._group == NLGroupAssignment::NO_GROUP) {
            break;
        } else if (cell._key == key) {
            return cell._group;
        }

        slot = (slot + 1) & mask;
    }

    const size_t group = assignment.createGroup(row);

    Cell& created = _cells[slot];
    created._key = key;
    created._group = group;
    _filledCells++;

    if (2 * _filledCells > _cells.size()) {
        growCells(_cells, [](const Cell& cell) { return hashPackedKey(cell._key); });
    }

    return group;
}

NLByteKeyCells::NLByteKeyCells()
    : _cells(INITIAL_CELL_COUNT)
{
}

NLByteKeyCells::~NLByteKeyCells() {
}

size_t NLByteKeyCells::findOrCreateGroup(std::string_view key, size_t row, NLGroupAssignment& assignment) {
    const uint64_t hash = std::hash<std::string_view> {}(key);
    const size_t mask = _cells.size() - 1;

    size_t slot = hash & mask;

    while (true) {
        const Cell& cell = _cells[slot];

        if (cell._group == NLGroupAssignment::NO_GROUP) {
            break;
        } else if (cell._hash == hash && cell._key == key) {
            return cell._group;
        }

        slot = (slot + 1) & mask;
    }

    const size_t group = assignment.createGroup(row);

    Cell& created = _cells[slot];
    created._hash = hash;
    created._group = group;
    created._key = _keys.own(key);
    _filledCells++;

    if (2 * _filledCells > _cells.size()) {
        growCells(_cells, [](const Cell& cell) { return cell._hash; });
    }

    return group;
}

void NLByteKeyCells::clear() {
    _cells.assign(INITIAL_CELL_COUNT, Cell {});
    _filledCells = 0;
    _keys.clear();
}

template <typename Element>
NLStringGroupHashTable<Element>::NLStringGroupHashTable() {
}

template <typename Element>
NLStringGroupHashTable<Element>::~NLStringGroupHashTable() {
}

template <typename Element>
NLGroupHashTableKind NLStringGroupHashTable<Element>::getKind() const {
    if constexpr (std::same_as<Element, std::string_view>) {
        return NLGroupHashTableKind::String;
    } else {
        return NLGroupHashTableKind::NullableString;
    }
}

template <typename Element>
void NLStringGroupHashTable<Element>::assign(std::span<const Column* const> keyColumns, NLGroupAssignment& assignment) {
    const std::vector<Element>& inputRaw = static_cast<const ColumnVector<Element>*>(keyColumns.front())->getRaw();
    const size_t rowCount = assignment.getRowCount();

    for (size_t row = 0; row < rowCount; row++) {
        const Element& element = inputRaw[row];

        if constexpr (std::same_as<Element, std::string_view>) {
            assignment.setGroup(row, _cells.findOrCreateGroup(element, row, assignment));
        } else if (element.has_value()) {
            assignment.setGroup(row, _cells.findOrCreateGroup(*element, row, assignment));
        } else {
            assignment.assignNullGroup(row, &_nullGroup);
        }
    }
}

template <typename Element>
void NLStringGroupHashTable<Element>::clear() {
    _cells.clear();
    _nullGroup = NLGroupAssignment::NO_GROUP;
}

NLSerializedGroupHashTable::NLSerializedGroupHashTable() {
}

NLSerializedGroupHashTable::~NLSerializedGroupHashTable() {
}

void NLSerializedGroupHashTable::assign(std::span<const Column* const> keyColumns, NLGroupAssignment& assignment) {
    const size_t rowCount = assignment.getRowCount();
    const size_t keyCount = _keyAppends.size();

    for (size_t row = 0; row < rowCount; row++) {
        _rowKey.clear();
        for (size_t keyIndex = 0; keyIndex < keyCount; keyIndex++) {
            _keyAppends[keyIndex](keyColumns[keyIndex], row, _rowKey);
        }

        assignment.setGroup(row, _cells.findOrCreateGroup(_rowKey.getBytes(), row, assignment));
    }
}

void NLSerializedGroupHashTable::clear() {
    _cells.clear();
}

NLGroupHashTable::NLGroupHashTable() {
}

NLGroupHashTable::~NLGroupHashTable() {
}

void NLGroupHashTable::chooseTable() {
    if (_keys.empty()) {
        _table.emplace<NLKeylessGroupHashTable>();
        return;
    }

    const NLGroupHashKey& firstKey = _keys.front();
    const bool oneKey = _keys.size() == 1;

    size_t nullableCount = 0;
    size_t valueBytes = 0;
    bool allFixed = true;

    for (const NLGroupHashKey& key : _keys) {
        allFixed = allFixed && (key._shape == NLGroupKeyShape::Fixed);
        nullableCount += key._nullable ? 1 : 0;
        valueBytes += key._packing._width;
    }

    const size_t bitmapBytes = (nullableCount + 7) / 8;
    const size_t packedBytes = bitmapBytes + valueBytes;
    const bool nullAside = oneKey && allFixed && firstKey._nullable;

    if (oneKey && firstKey._shape == NLGroupKeyShape::String) {
        if (firstKey._nullable) {
            _table.emplace<NLStringGroupHashTable<std::optional<std::string_view>>>();
        } else {
            _table.emplace<NLStringGroupHashTable<std::string_view>>();
        }
    } else if (nullAside && valueBytes <= sizeof(uint8_t)) {
        choosePackedTable<NLDirectGroupHashTable<uint8_t, true>>(0);
    } else if (nullAside && valueBytes <= sizeof(uint16_t)) {
        choosePackedTable<NLDirectGroupHashTable<uint16_t, true>>(0);
    } else if (nullAside && valueBytes <= sizeof(NLPackedKey<1>)) {
        choosePackedTable<NLPackedGroupHashTable<1, true>>(0);
    } else if (allFixed && packedBytes <= sizeof(uint8_t)) {
        choosePackedTable<NLDirectGroupHashTable<uint8_t, false>>(bitmapBytes);
    } else if (allFixed && packedBytes <= sizeof(uint16_t)) {
        choosePackedTable<NLDirectGroupHashTable<uint16_t, false>>(bitmapBytes);
    } else if (allFixed && packedBytes <= sizeof(NLPackedKey<1>)) {
        choosePackedTable<NLPackedGroupHashTable<1, false>>(bitmapBytes);
    } else if (allFixed && packedBytes <= sizeof(NLPackedKey<2>)) {
        choosePackedTable<NLPackedGroupHashTable<2, false>>(bitmapBytes);
    } else if (allFixed && packedBytes <= sizeof(NLPackedKey<4>)) {
        choosePackedTable<NLPackedGroupHashTable<4, false>>(bitmapBytes);
    } else {
        NLSerializedGroupHashTable& table = _table.emplace<NLSerializedGroupHashTable>();

        for (const NLGroupHashKey& key : _keys) {
            table.addKeyAppend(key._keyAppend);
        }
    }
}

NLGroupHashTableKind NLGroupHashTable::getKind() const {
    return std::visit([](const auto& table) { return table.getKind(); }, _table);
}

void NLGroupHashTable::assign(std::span<const Column* const> keyColumns, size_t rowCount) {
    std::vector<size_t>& rowGroups = _rowGroups.getRaw();
    rowGroups.resize(rowCount);
    _newGroupRows.clear();

    NLGroupAssignment assignment(rowGroups, _newGroupRows, _groupCount);
    std::visit([&](auto& table) { table.assign(keyColumns, assignment); }, _table);
}

void NLGroupHashTable::buildRowGroupKey(NLGroupHashKey& key) {
    key._shape = NLGroupKeyShape::Fixed;
    key._nullable = false;
    key._packing = NLKeyPacking {&packRowGroupColumn, sizeof(size_t)};
    key._keyAppend = &appendRowGroupKey;
}

// Without a key every row has the empty key, so its one group is there whether or not a row
// arrives: collect() over no row is the empty list, not the absence of a row
void NLGroupHashTable::clear() {
    std::visit([](auto& table) { table.clear(); }, _table);

    _groupCount = _keys.empty() ? 1 : 0;
}

// The values of each key start after a null bitmap of bitmapBytes, which is 0 for a table
// holding its key's nulls aside
template <typename PackedTable>
void NLGroupHashTable::choosePackedTable(size_t bitmapBytes) {
    PackedTable& table = _table.emplace<PackedTable>();

    size_t valueOffset = bitmapBytes;
    size_t nullBit = 0;

    for (const NLGroupHashKey& key : _keys) {
        NLPackedKeyColumn packed;
        packed._pack = key._packing._pack;
        packed._valueOffset = valueOffset;

        if (key._nullable) {
            packed._nullBit = nullBit;
            nullBit++;
        }

        table.addKeyColumn(packed);
        valueOffset += key._packing._width;
    }
}

namespace db {

template class NLDirectGroupHashTable<uint8_t, false>;
template class NLDirectGroupHashTable<uint8_t, true>;
template class NLDirectGroupHashTable<uint16_t, false>;
template class NLDirectGroupHashTable<uint16_t, true>;
template class NLPackedGroupHashTable<1, false>;
template class NLPackedGroupHashTable<1, true>;
template class NLPackedGroupHashTable<2, false>;
template class NLPackedGroupHashTable<4, false>;
template class NLStringGroupHashTable<std::string_view>;
template class NLStringGroupHashTable<std::optional<std::string_view>>;

}
