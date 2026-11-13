#pragma once

#include <algorithm>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>

#include "OutputValues.h"
#include "TuringProtoNestedWriter.h"
#include "TuringProtoOutBuf.h"
#include "TuringProtoHeaders.h"
#include "columns/ColumnOptVector.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "QueryCallbacks.h"
#include "Bitmask.h"
#include "FatalException.h"

namespace db {
class QueryStatus;
}

namespace net::proto {

struct ColInternalKindToProtoEnum {
    template <typename T>
    static constexpr auto map() {
        using Enum = net::proto::ColumnInternalKind;
        if constexpr (std::is_same_v<T, db::NodeID>) {
            return Enum::NODE_ID;
        } else if constexpr (std::is_same_v<T, db::EdgeID>) {
            return Enum::EDGE_ID;
        } else if constexpr (std::is_same_v<T, db::EdgeTypeID>) {
            return Enum::EDGE_TYPE_ID;
        } else if constexpr (std::is_same_v<T, db::PropertyTypeID>) {
            return Enum::PROPERTY_TYPE_ID;
        } else if constexpr (std::is_same_v<T, db::LabelID>) {
            return Enum::LABEL_ID;
        } else if constexpr (std::is_same_v<T, db::LabelSetID>) {
            return Enum::LABEL_SET_ID;
        } else if constexpr (std::is_same_v<T, db::CommitHash>) {
            return Enum::COMMIT_HASH;
        } else if constexpr (std::is_same_v<T, db::ChangeID>) {
            return Enum::CHANGE_ID;
        } else if constexpr (std::unsigned_integral<T> || db::OptionalUnsignedInteger<T>) {
            return Enum::UINT64;
        } else if constexpr (db::IsInt64<T>) {
            return Enum::INT64;
        } else if constexpr (db::IsFloat64<T>) {
            return Enum::DOUBLE;
        } else if constexpr (db::IsString<T>) {
            return Enum::STRING;
        } else if constexpr (db::IsBool<T>) {
            return Enum::BOOL;
        } else if constexpr (db::IsPath<T>) {
            return Enum::GRAPH_PATH;
        } else if constexpr (db::IsEmbedding<T>) {
            return Enum::EMBEDDING;
        } else if constexpr (db::IsDateTime<T>) {
            return Enum::DATE_TIME;
        } else if constexpr (db::IsDuration<T>) {
            return Enum::DURATION;
        } else if constexpr (db::IsPoint<T>) {
            return Enum::POINT;
        } else if constexpr (db::IsListView<T>) {
            return Enum::LIST_VIEW;
        } else if constexpr (db::IsMap<T>) {
            return Enum::MAP_VIEW;
        } else if constexpr (db::IsCell<T>) {
            return Enum::CELL_VIEW;
        } else if constexpr (db::IsValueType<T>) {
            return Enum::VALUE_TYPE;
        } else if constexpr (db::IsNull<T>) {
            return Enum::PROPERTY_NULL;
        } else {
            static_assert(sizeof(T) == 0, "No mapping for this type");
        }
    }
};

struct ColumnHeaderWriter {
    net::proto::ColumnWireHeader& _header;

    ColumnHeaderWriter() = delete;
    explicit ColumnHeaderWriter(net::proto::ColumnWireHeader& header)
        : _header(header)
    {
    }

    void writeColumnSchema(net::proto::ColumnInternalKind typeCode,
                           net::proto::ColumnKind encoding) {
        _header._typeCode= std::to_underlying(typeCode);
        _header._encoding = std::to_underlying(encoding);
    }

    template <typename T>
    void operator()(const db::ColumnVector<T>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        const net::proto::ColumnKind encoding = db::EntityIDLike<T> ? net::proto::ColumnKind::OPTIONAL_VECTOR
                                                                    : net::proto::ColumnKind::VECTOR;
        writeColumnSchema(typeCode, encoding);
    }

    template <typename T>
    void operator()(const db::ColumnOptVector<T>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        writeColumnSchema(typeCode, net::proto::ColumnKind::OPTIONAL_VECTOR);
    }

    template <typename T>
    void operator()(const db::ColumnConst<T>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        writeColumnSchema(typeCode, net::proto::ColumnKind::CONSTANT);
    }

    template <typename T>
    void operator()(const db::ColumnConst<std::optional<T>>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        writeColumnSchema(typeCode, net::proto::ColumnKind::OPTIONAL_CONSTANT);
    }
};


class DataWriter {
public:
    DataWriter(net::proto::TuringProtoOutBuf* outBuf,
               NestedContainerWriter& nestedWriter,
               size_t offset,
               size_t rowCount)
        : _outBuf(outBuf),
        _nestedWriter(nestedWriter),
        _offset(offset),
        _rowCount(rowCount)
    {
    }

    ~DataWriter() = default;

    void writeRowCount(size_t size) {
        bioassert(size <= MAX_WIRE_SIZE, "Number of data frame rows is too high");
        const WireSize rowCount = static_cast<WireSize>(size);
        _outBuf->copyFixedLenData(&rowCount, sizeof(rowCount));
    }

    template <typename T>
    void operator()(const db::ColumnVector<T>* col) {
        const std::span<const T> values = columnWindow(col->data(), col->size());

        writeRowCount(values.size());

        if constexpr (db::EntityIDLike<T>) {
            DynamicLargeBitMask<uint64_t> mask(values.size());
            for (size_t row = 0; row < values.size(); row++) {
                mask.set(row, values[row].isValid());
            }
            _outBuf->copyVarLenData(mask.data(), mask.byteSize());

            const size_t columnByteSize = sizeof(T) * values.size();
            _outBuf->copyVector<T>(values.data(), columnByteSize);
        } else if constexpr (db::StringLike<T>) {
            for (const auto& val : values) {
                bioassert(val.size() * sizeof(char) <= MAX_WIRE_SIZE, "String length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(char));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val.data(), columnByteSize);
            }
        } else if constexpr (db::IsPath<T>) {
            for (const db::GraphPath& path : values) {
                writePath(path);
            }
        } else if constexpr (db::IsEmbedding<T>) {
            for (const auto& val : values) {
                bioassert(val.size() * sizeof(float) <= MAX_WIRE_SIZE, "Embedding length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(float));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val.data(), columnByteSize);
            }
        } else if constexpr (db::IsListView<T>) {
            for (const auto& listView : values) {
                _nestedWriter.writeListView(listView);
            }
        } else if constexpr (db::IsMap<T>) {
            for (const auto& mapView : values) {
                _nestedWriter.writeMapView(mapView);
            }
        } else if constexpr (db::IsCell<T>) {
            // One element per row: the row count (already written above) is the element
            // count, so only [listByteSize] + the elements follow.
            _nestedWriter.writeCells(values);
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder can't encode a non trivial element in a trivial manner");
            const size_t columnByteSize = sizeof(T) * values.size();
            _outBuf->copyVector<T>(values.data(), columnByteSize);
        }
    }

    template <typename T>
    void operator()(const db::ColumnOptVector<T>* col) {
        const size_t rowCount = windowRowCount(col->size());
        const size_t endRow = _offset + rowCount;

        writeRowCount(rowCount);

        DynamicLargeBitMask<uint64_t> mask(rowCount);
        for (size_t row = 0; row < rowCount; row++) {
            mask.set(row, col->isValid(_offset + row));
        }
        _outBuf->copyVarLenData(mask.data(), mask.byteSize());

        if constexpr (db::StringLike<T>) {
            for (size_t row = _offset; row < endRow; row++) {
                const auto& value = col->getValue(row);
                bioassert(value.size() * sizeof(char) <= MAX_WIRE_SIZE, "Optional string length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(value.size() * sizeof(char));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(value.data(), columnByteSize);
            }
        } else if constexpr (db::IsEmbedding<T>) {
            for (size_t row = _offset; row < endRow; row++) {
                const auto& value = col->getValue(row);
                bioassert(value.size() * sizeof(float) <= MAX_WIRE_SIZE, "Optional embedding length exceeds maximum wire size");
                const WireSize columnByteSize = static_cast<WireSize>(value.size() * sizeof(float));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(value.data(), columnByteSize);
            }
        } else if constexpr (db::IsListView<T>) {
            // A row with no list still carries the [count][listByteSize] header, empty, so
            // every row is the same shape on the wire and the mask alone says which is null.
            for (size_t row = _offset; row < endRow; row++) {
                _nestedWriter.writeListView(col->getValue(row));
            }
        } else if constexpr (db::IsMap<T>) {
            for (size_t row = _offset; row < endRow; row++) {
                _nestedWriter.writeMapView(col->getValue(row));
            }
        } else if constexpr (db::IsCell<T>) {
            _nestedWriter.writeOptionalCells(col, _offset, rowCount);
        } else if constexpr (db::IsNull<T>) {
            // Don't send anything for property null
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder only supports trivially copyable types and string");
            for (size_t row = _offset; row < endRow; row++) {
                _outBuf->copyFixedLenData(&col->getValue(row), sizeof(T));
            }
        }
    }

    template <typename T>
    void operator()(const db::ColumnConst<T>* col) {
        if constexpr (db::StringLike<T>) {
            const auto& val = col->at(0);
            bioassert(val.size() * sizeof(char) <= MAX_WIRE_SIZE, "Const string length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(char));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsPath<T>) {
            writePath(col->at(0));
        } else if constexpr (db::IsEmbedding<T>) {
            const auto& val = col->at(0);
            bioassert(val.size() * sizeof(float) <= MAX_WIRE_SIZE, "Const embedding length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(val.size() * sizeof(float));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsListView<T>) {
            _nestedWriter.writeListView(col->at(0));
        } else if constexpr (db::IsMap<T>) {
            _nestedWriter.writeMapView(col->at(0));
        } else if constexpr (db::IsCell<T>) {
            const db::CellView element = col->at(0);
            _nestedWriter.writeCells(std::span<const db::CellView>(&element, 1));
        } else if constexpr (db::IsNull<T>) {
            // Don't send anything for property null
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder only supports trivially copyable types and string");
            _outBuf->copyFixedLenData(&col->at(0), sizeof(T));
        }
    }

    template <typename T>
    void operator()(const db::ColumnConst<std::optional<T>>* col) {
        if (col->empty()) {
            throw FatalException("An empty constant column holds no value to encode");
        }

        const uint8_t hasValue = col->isValid(0) ? 1 : 0;
        _outBuf->copyFixedLenData(&hasValue, sizeof(hasValue));

        if (!hasValue) {
            return;
        }

        const T& value = col->getValue(0);

        if constexpr (db::StringLike<T>) {
            bioassert(value.size() * sizeof(char) <= MAX_WIRE_SIZE, "Optional const string length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(value.size() * sizeof(char));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(value.data(), columnByteSize);
        } else if constexpr (db::IsEmbedding<T>) {
            bioassert(value.size() * sizeof(float) <= MAX_WIRE_SIZE, "Optional const embedding length exceeds maximum wire size");
            const WireSize columnByteSize = static_cast<WireSize>(value.size() * sizeof(float));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(value.data(), columnByteSize);
        } else if constexpr (db::IsListView<T>) {
            _nestedWriter.writeListView(value);
        } else if constexpr (db::IsMap<T>) {
            _nestedWriter.writeMapView(value);
        } else if constexpr (db::IsCell<T>) {
            _nestedWriter.writeCells(std::span<const db::CellView>(&value, 1));
        } else if constexpr (db::IsNull<T>) {
            // Don't send anything for property null
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder only supports trivially copyable types and string");
            _outBuf->copyFixedLenData(&value, sizeof(T));
        }
    }

private:
    net::proto::TuringProtoOutBuf* _outBuf {nullptr};
    NestedContainerWriter& _nestedWriter;
    size_t _offset {0};
    size_t _rowCount {0};

    // We don't assume dataframes to be rectangular in the encoder. For each column we
    // calculate how many rows of the window it holds.
    size_t windowRowCount(size_t columnSize) const {
        const size_t available = columnSize > _offset ? columnSize - _offset : 0;

        return std::min(_rowCount, available);
    }

    template <typename T>
    std::span<const T> columnWindow(const T* data, size_t columnSize) const {
        return std::span<const T>(data + _offset, windowRowCount(columnSize));
    }

    void writePath(const db::GraphPath& path) {
        const std::span<const uint64_t> ids = path.getIDs();
        bioassert(ids.size_bytes() <= MAX_WIRE_SIZE, "Path length exceeds maximum wire size");

        const WireSize nodeCount = static_cast<WireSize>(path.getNodeCount());
        _outBuf->copyFixedLenData(&nodeCount, sizeof(nodeCount));

        if (path.isNull()) {
            return;
        }

        _outBuf->copyVarLenData(ids.data(), ids.size_bytes());
    }
};

class TuringProtoEncoder {
public:
    explicit TuringProtoEncoder(net::proto::TuringProtoOutBuf* outBuf);

    void writeColumnHeaders(std::span<const std::string_view> names,
                            std::span<const db::Column* const> columns);
    void writeColumns(std::span<const db::Column* const> columns, size_t offset, size_t rowCount);
    //A chunk footer contains metadata about the columns which are not needed for the decoding
    //of the columns - for now this only includes the rowCount.
    void writeChunkFooter(size_t rowCount);
    void writeError(const db::QueryStatus* status);
    void writeProtocolError(std::string_view message);
    void writeEnd(db::QueryCallbacks::ExecTimeMilliseconds milliseconds);

private:
    net::proto::TuringProtoOutBuf* _outBuf {nullptr};
    NestedContainerWriter _nestedWriter;

    void writeColumnCount(size_t count);
    void writeColumnHeader(std::string_view name, const db::Column* column);
};

}
