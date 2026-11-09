#pragma once

#include <stddef.h>
#include <stdint.h>

#include <cstddef>
#include <limits>
#include <optional>
#include <span>
#include <string_view>
#include <variant>
#include <vector>

#include "NLSerializedKey.h"
#include "NLValueStore.h"
#include "columns/ColumnVector.h"

namespace db {

class Column;

// Type of handle that appends the raw bytes of one row of a column to a growing
// row-key buffer. One per column kind / value type, selected during translation,
// the same way the gather and append families are. The concatenated key of a row
// (all its columns in order) is what a serialized group hash table keys the row by:
// bytewise key equality is row equality. A null serializes to a fixed tag byte, so
// all nulls share a key and DISTINCT dedups them together, matching Cypher; a
// string serializes as a length prefix then its characters, so two rows never
// collide by concatenation (e.g. "a"+"b" versus "ab"+"").
using NLKeyAppendFunction = void (*)(const Column* column, size_t row, NLSerializedKey& key);

// Where a key column writes the keys of a batch of rows: the value of the batch's row i at
// _values + i * _valueStride, and a null as bit _nullBit of the bytes at _nulls + i * _nullStride.
// A packed key opens with its null bitmap, so both strides are the key's size; the nulls of a
// key held aside are one flag per row.
struct NLKeyPackTarget {
    std::byte* _values {nullptr};
    size_t _valueStride {0};
    std::byte* _nulls {nullptr};
    size_t _nullStride {0};
    size_t _nullBit {0};
};

// Type of handle that writes the keys of rows [beginRow, endRow) of a key column whose values
// have a fixed width: each value's bytes, normalized so two values one group takes pack alike, or
// for a null the column's null bit, its value bytes left zero. One per column kind / value type,
// selected during translation.
using NLKeyPackFunction = void (*)(const Column* column,
                                   size_t beginRow,
                                   size_t endRow,
                                   const NLKeyPackTarget& target);

// The packing of a key column's values: the handle writing them and the bytes each takes
struct NLKeyPacking {
    NLKeyPackFunction _pack {nullptr};
    size_t _width {0};

    bool operator==(const NLKeyPacking& other) const = default;
};

// What a key column's values are to a hash table: of a fixed width, which packs them into one
// integer with the other keys' values, a string, or anything else, which only serializes
enum class NLGroupKeyShape {
    Fixed,
    String,
    Other,
};

// A key column as a group hash table reads it: how its values reach the table. Two columns of one
// chunk type read alike, so any of them can key the same table.
struct NLGroupHashKey {
    NLGroupKeyShape _shape {NLGroupKeyShape::Other};
    bool _nullable {false};
    NLKeyPacking _packing;
    NLKeyAppendFunction _keyAppend {nullptr};

    bool operator==(const NLGroupHashKey& other) const = default;
};

// The table a group hash table maps the keys of its rows to groups with, chosen from the key
// columns as ClickHouse's Aggregator chooses its AggregatedDataVariants
enum class NLGroupHashTableKind {
    Keyless,
    Direct8,
    NullableDirect8,
    Direct16,
    NullableDirect16,
    Fixed64,
    NullableFixed64,
    Fixed128,
    Fixed256,
    String,
    NullableString,
    Serialized,
};

// The rows of a step mapped to their groups. A group the step creates takes the next dense index,
// and the row that created it is kept.
class NLGroupAssignment {
public:
    static constexpr size_t NO_GROUP = std::numeric_limits<size_t>::max();

    NLGroupAssignment(std::vector<size_t>& groups, std::vector<size_t>& newGroupRows, size_t& groupCount);
    ~NLGroupAssignment();

    size_t getRowCount() const { return _groups.size(); }

    void setGroup(size_t row, size_t group) { _groups[row] = group; }
    size_t createGroup(size_t row);
    void assignNullGroup(size_t row, size_t* nullGroup);

private:
    std::vector<size_t>& _groups;
    std::vector<size_t>& _newGroupRows;
    size_t& _groupCount;
};

// The table of a group hash table without a key: every row falls in the one group of the empty key
class NLKeylessGroupHashTable {
public:
    NLGroupHashTableKind getKind() const { return NLGroupHashTableKind::Keyless; }

    void assign(std::span<const Column* const> keyColumns, NLGroupAssignment& assignment);
    void clear();
};

template <size_t Words>
struct NLPackedKey {
    uint64_t _words[Words] {};

    bool operator==(const NLPackedKey& other) const = default;
};

struct NLPackedKeyColumn {
    NLKeyPackFunction _pack {nullptr};
    size_t _valueOffset {0};
    size_t _nullBit {0};
};

// The keys of a batch of rows, each packing the values of every key column into one Key. A
// nullable column adds a bit to the null bitmap the key opens with. With NullAside there is one
// key column and it is nullable: its value alone fills the key, and its nulls are flagged beside.
template <typename Key, bool NullAside>
class NLPackedKeyBatch {
public:
    NLPackedKeyBatch();
    ~NLPackedKeyBatch();

    void addKeyColumn(const NLPackedKeyColumn& column) { _keyColumns.push_back(column); }

    void pack(std::span<const Column* const> keyColumns, size_t beginRow, size_t endRow);

    const std::vector<Key>& getKeys() const { return _keys; }
    bool isNull(size_t index) const requires NullAside { return _nulls[index] != std::byte {0}; }

private:
    std::vector<NLPackedKeyColumn> _keyColumns;
    std::vector<Key> _keys;
    std::vector<std::byte> _nulls;
};

// The table of keys that pack into one or two bytes, ClickHouse's key8 and key16: the packed key
// indexes an array of 256 or 65,536 groups, so a row is neither hashed nor probed. With NullAside
// the one key's nulls fall in a group held beside the array.
template <typename Key, bool NullAside>
class NLDirectGroupHashTable {
public:
    NLDirectGroupHashTable();
    ~NLDirectGroupHashTable();

    NLGroupHashTableKind getKind() const;

    void addKeyColumn(const NLPackedKeyColumn& column) { _batch.addKeyColumn(column); }

    void assign(std::span<const Column* const> keyColumns, NLGroupAssignment& assignment);
    void clear();

private:
    NLPackedKeyBatch<Key, NullAside> _batch;
    std::vector<size_t> _groups;
    std::vector<Key> _groupKeys;
    size_t _nullGroup {NLGroupAssignment::NO_GROUP};
};

// The table of keys whose values all have a fixed width, packed into one key of Words 64-bit
// words: ClickHouse's keys64, keys128 and keys256. With NullAside the one key's nulls fall in a
// group held beside the table, as ClickHouse's nullable_key64 holds them.
template <size_t Words, bool NullAside>
class NLPackedGroupHashTable {
public:
    using Key = NLPackedKey<Words>;

    NLPackedGroupHashTable();
    ~NLPackedGroupHashTable();

    NLGroupHashTableKind getKind() const;

    void addKeyColumn(const NLPackedKeyColumn& column) { _batch.addKeyColumn(column); }

    void assign(std::span<const Column* const> keyColumns, NLGroupAssignment& assignment);
    void clear();

private:
    struct Cell {
        Key _key;
        size_t _group {NLGroupAssignment::NO_GROUP};
    };

    NLPackedKeyBatch<Key, NullAside> _batch;
    std::vector<uint64_t> _batchHashes;
    std::vector<Cell> _cells;
    size_t _filledCells {0};
    size_t _nullGroup {NLGroupAssignment::NO_GROUP};

    Key _lastKey;
    size_t _lastGroup {NLGroupAssignment::NO_GROUP};

    size_t findOrCreateGroup(const Key& key, uint64_t hash, size_t row, NLGroupAssignment& assignment);
};

// The cells of a table keying groups by bytes: each holds a group, a copy of its key's bytes and
// their hash
class NLByteKeyCells {
public:
    NLByteKeyCells();
    ~NLByteKeyCells();

    size_t findOrCreateGroup(std::string_view key, size_t row, NLGroupAssignment& assignment);
    void clear();

private:
    struct Cell {
        uint64_t _hash {0};
        size_t _group {NLGroupAssignment::NO_GROUP};
        std::string_view _key;
    };

    std::vector<Cell> _cells;
    size_t _filledCells {0};
    NLValueStore _keys;
};

// The table of one string key, ClickHouse's key_string. Element is the column's element, a
// string or a nullable one, whose nulls fall in a group held beside the table.
template <typename Element>
class NLStringGroupHashTable {
public:
    NLStringGroupHashTable();
    ~NLStringGroupHashTable();

    NLGroupHashTableKind getKind() const;

    void assign(std::span<const Column* const> keyColumns, NLGroupAssignment& assignment);
    void clear();

private:
    NLByteKeyCells _cells;
    size_t _nullGroup {NLGroupAssignment::NO_GROUP};
};

// The table of keys no other table holds - a list, a map, a cell, a string beside another key -
// ClickHouse's serialized: a row's key is serialized column by column into bytes.
class NLSerializedGroupHashTable {
public:
    NLSerializedGroupHashTable();
    ~NLSerializedGroupHashTable();

    NLGroupHashTableKind getKind() const { return NLGroupHashTableKind::Serialized; }

    void addKeyAppend(NLKeyAppendFunction keyAppend) { _keyAppends.push_back(keyAppend); }

    void assign(std::span<const Column* const> keyColumns, NLGroupAssignment& assignment);
    void clear();

private:
    std::vector<NLKeyAppendFunction> _keyAppends;
    NLByteKeyCells _cells;
    NLSerializedKey _rowKey;
};

// Maps the keys of each row of a step to the dense index of its group, 0, 1, 2... in the order
// the groups appear, through a table chosen from the key columns once they are all added
class NLGroupHashTable {
public:
    NLGroupHashTable();
    ~NLGroupHashTable();

    void addKey(const NLGroupHashKey& key) { _keys.push_back(key); }
    const std::vector<NLGroupHashKey>& getKeys() const { return _keys; }

    void chooseTable();
    NLGroupHashTableKind getKind() const;

    // Map each of the rowCount rows of keyColumns, one chunk per key, to its group, creating a
    // group for each key seen for the first time
    void assign(std::span<const Column* const> keyColumns, size_t rowCount);

    // The group of each row the last assign mapped, and the rows that created a group, in order
    const ColumnVector<size_t>* getRowGroupColumn() const { return &_rowGroups; }
    const std::vector<size_t>& getRowGroups() const { return _rowGroups.getRaw(); }
    const std::vector<size_t>& getNewGroupRows() const { return _newGroupRows; }

    // Key a column of group indices, such as getRowGroupColumn's, in another table
    static void buildRowGroupKey(NLGroupHashKey& key);

    size_t getGroupCount() const { return _groupCount; }

    void clear();

private:
    using Table = std::variant<NLKeylessGroupHashTable,
                               NLDirectGroupHashTable<uint8_t, false>,
                               NLDirectGroupHashTable<uint8_t, true>,
                               NLDirectGroupHashTable<uint16_t, false>,
                               NLDirectGroupHashTable<uint16_t, true>,
                               NLPackedGroupHashTable<1, false>,
                               NLPackedGroupHashTable<1, true>,
                               NLPackedGroupHashTable<2, false>,
                               NLPackedGroupHashTable<4, false>,
                               NLStringGroupHashTable<std::string_view>,
                               NLStringGroupHashTable<std::optional<std::string_view>>,
                               NLSerializedGroupHashTable>;

    std::vector<NLGroupHashKey> _keys;
    Table _table;
    size_t _groupCount {0};
    ColumnVector<size_t> _rowGroups;
    std::vector<size_t> _newGroupRows;

    template <typename PackedTable>
    void choosePackedTable(size_t bitmapBytes);
};

}
