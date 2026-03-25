#pragma once

#include <type_traits>
#include <utility>

#include "Bitmask.h"
#include "OutputValues.h"
#include "TuringProtoOutBuf.h"
#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"
#include "metadata/PropertyType.h"
#include "TuringProtoHeaders.h"
#include "QueryCallbacks.h"

namespace db {
class QueryStatus;
}

namespace net::proto {

using ProtoEncoderSupportedTypes = std::tuple<
    db::types::Int64::Primitive,
    db::types::UInt64::Primitive,
    db::types::Double::Primitive,
    db::types::String::Primitive,
    db::types::Bool::Primitive,
    db::types::Embedding::Primitive,
    std::optional<db::types::Int64::Primitive>,
    std::optional<db::types::UInt64::Primitive>,
    std::optional<db::types::Double::Primitive>,
    std::optional<db::types::String::Primitive>,
    std::optional<db::types::Bool::Primitive>,
    std::optional<db::types::Embedding::Primitive>,
    db::NodeID,
    db::EdgeID,
    db::LabelID,
    db::LabelSetID,
    db::EdgeTypeID,
    db::PropertyTypeID,
    db::ChangeID,
    db::TemplateCommitHash<0>,
    db::TemplateCommitHash<1>,
    size_t,
    std::string,
    db::Path,
    db::EntityList,
    db::PropertyNull,
    db::ValueType>;

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
            return Enum::PATH;
        } else if constexpr (db::IsEmbedding<T>) {
            return Enum::EMBEDDING;
        } else if constexpr (db::IsList<T>) {
            return Enum::ENTITY_LIST;
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
        writeColumnSchema(typeCode, net::proto::ColumnKind::VECTOR);
    }

    template <typename T>
    void operator()(const db::ColumnVector<std::optional<T>>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        writeColumnSchema(typeCode, net::proto::ColumnKind::OPTIONAL_VECTOR);
    }

    template <typename T>
    void operator()(const db::ColumnConst<T>* col) {
        const auto typeCode = ColInternalKindToProtoEnum::map<T>();
        writeColumnSchema(typeCode, net::proto::ColumnKind::CONSTANT);
    }
};

class DataWriter {
public:
    explicit DataWriter(net::proto::TuringProtoOutBuf* outBuf)
        : _outBuf(outBuf)
    {
    }

    ~DataWriter() = default;

    void writeRowCount(size_t size) {
        bioassert(size <= std::numeric_limits<uint32_t>::max(), "Number of data frame rows is too high");
        const uint32_t rowCount = static_cast<uint32_t>(size);
        _outBuf->copyFixedLenData(&rowCount, sizeof(rowCount));
    }

    template <typename T>
    void operator()(const db::ColumnVector<T>* col) {
        writeRowCount(col->size());

        if constexpr (db::StringLike<T>) {
            for (const auto& val : *col) {
                bioassert(val.size() * sizeof(char) <= std::numeric_limits<uint32_t>::max(), "String length exceeds maximum wire size");
                const uint32_t columnByteSize = static_cast<uint32_t>(val.size() * sizeof(char));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val.data(), columnByteSize);
            }
        } else if constexpr (db::IsPath<T>) {
            for (const auto& val : *col) {
                bioassert(val.size() * sizeof(db::EntityID) <= std::numeric_limits<uint32_t>::max(), "Path length exceeds maximum wire size");
                const uint32_t columnByteSize = static_cast<uint32_t>(val.size() * sizeof(db::EntityID));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val.data(), columnByteSize);
            }
        } else if constexpr (db::IsEmbedding<T>) {
            for (const auto& val : *col) {
                bioassert(val.size() * sizeof(float) <= std::numeric_limits<uint32_t>::max(), "Embedding length exceeds maximum wire size");
                const uint32_t columnByteSize = static_cast<uint32_t>(val.size() * sizeof(float));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val.data(), columnByteSize);
            }
        } else if constexpr (db::IsList<T>) {
            constexpr size_t sizeOfEntry = sizeof(db::EntityList::Entry::_id) + sizeof(db::EntityList::Entry::_type);

            for (const auto& entityList : *col) {
                bioassert(entityList.size() <= std::numeric_limits<uint32_t>::max(), "Entity list length exceeds maximum wire size");
                const uint32_t columnByteSize = static_cast<uint32_t>(entityList.size());
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                for (const auto& entry : entityList) {
                    // Ensure that we can copy a full entry into the packet
                    _outBuf->checkRemainingAndFlush(sizeOfEntry);

                    _outBuf->copyFixedLenData(&entry._type, sizeof(entry._type));
                    _outBuf->copyFixedLenData(&entry._id, sizeof(entry._id));
                }
            }
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder only supports trivially copyable types and string");
            const size_t columnByteSize = sizeof(T) * col->size();
            _outBuf->copyVector<T>(col->data(), columnByteSize);
        }
    }

    template <typename T>
    void operator()(const db::ColumnVector<std::optional<T>>* col) {
        writeRowCount(col->size());

        DynamicLargeBitMask<uint64_t> mask(0);
        DynamicLargeBitMask<uint64_t>::create(mask, col->getRaw());
        _outBuf->copyVarLenData(mask.data(), mask.byteSize());

        if constexpr (db::StringLike<T>) {
            for (const auto& val : *col) {
                if (!val.has_value()) {
                    const uint32_t columnByteSize = 0;
                    _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                    continue;
                }

                bioassert(val->size() * sizeof(char) <= std::numeric_limits<uint32_t>::max(), "Optional string length exceeds maximum wire size");
                const uint32_t columnByteSize = static_cast<uint32_t>(val->size() * sizeof(char));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val->data(), columnByteSize);
            }
        } else if constexpr (db::IsPath<T>) {
            for (const auto& val : *col) {
                if (!val.has_value()) {
                    const uint32_t columnByteSize = 0;
                    _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                    continue;
                }

                bioassert(val->size() * sizeof(db::EntityID) <= std::numeric_limits<uint32_t>::max(), "Optional path length exceeds maximum wire size");
                const uint32_t columnByteSize = static_cast<uint32_t>(val->size() * sizeof(db::EntityID));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val->data(), columnByteSize);
            }
        } else if constexpr (db::IsEmbedding<T>) {
            for (const auto& val : *col) {
                if (!val.has_value()) {
                    const uint32_t columnByteSize = 0;
                    _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                    continue;
                }

                bioassert(val->size() * sizeof(float) <= std::numeric_limits<uint32_t>::max(), "Optional embedding length exceeds maximum wire size");
                const uint32_t columnByteSize = static_cast<uint32_t>(val->size() * sizeof(float));
                _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
                _outBuf->copyVarLenData(val->data(), columnByteSize);
            }
        } else if constexpr (db::IsList<T>) {
            // EntityList is only used as ColumnVector<EntityList>, never optional
        } else if constexpr (db::IsNull<T>) {
            // Don't send anything for property null
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder only supports trivially copyable types and string");
            for (const auto& val : *col) {
                if (!val.has_value()) {
                    const T zero {};
                    _outBuf->copyFixedLenData(&zero, sizeof(T));
                    continue;
                }

                _outBuf->copyFixedLenData(&*val, sizeof(T));
            }
        }
    }

    template <typename T>
    void operator()(const db::ColumnConst<T>* col) {
        if constexpr (db::StringLike<T>) {
            const auto& val = col->at(0);
            bioassert(val.size() * sizeof(char) <= std::numeric_limits<uint32_t>::max(), "Const string length exceeds maximum wire size");
            const uint32_t columnByteSize = static_cast<uint32_t>(val.size() * sizeof(char));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsPath<T>) {
            const auto& val = col->at(0);
            bioassert(val.size() * sizeof(db::EntityID) <= std::numeric_limits<uint32_t>::max(), "Const path length exceeds maximum wire size");
            const uint32_t columnByteSize = static_cast<uint32_t>(val.size() * sizeof(db::EntityID));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsEmbedding<T>) {
            const auto& val = col->at(0);
            bioassert(val.size() * sizeof(float) <= std::numeric_limits<uint32_t>::max(), "Const embedding length exceeds maximum wire size");
            const uint32_t columnByteSize = static_cast<uint32_t>(val.size() * sizeof(float));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsList<T>) {
            // EntityList is only used as ColumnVector<EntityList>, never const
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
        const auto& opt = col->at(0);
        const uint8_t hasValue = opt.has_value() ? 1 : 0;
        _outBuf->copyFixedLenData(&hasValue, sizeof(hasValue));

        if (!hasValue) {
            return;
        }

        if constexpr (db::StringLike<T>) {
            const auto& val = *opt;
            bioassert(val.size() * sizeof(char) <= std::numeric_limits<uint32_t>::max(), "Optional const string length exceeds maximum wire size");
            const uint32_t columnByteSize = static_cast<uint32_t>(val.size() * sizeof(char));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsPath<T>) {
            const auto& val = *opt;
            bioassert(val.size() * sizeof(db::EntityID) <= std::numeric_limits<uint32_t>::max(), "Optional const path length exceeds maximum wire size");
            const uint32_t columnByteSize = static_cast<uint32_t>(val.size() * sizeof(db::EntityID));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsEmbedding<T>) {
            const auto& val = *opt;
            bioassert(val.size() * sizeof(float) <= std::numeric_limits<uint32_t>::max(), "Optional const embedding length exceeds maximum wire size");
            const uint32_t columnByteSize = static_cast<uint32_t>(val.size() * sizeof(float));
            _outBuf->copyFixedLenData(&columnByteSize, sizeof(columnByteSize));
            _outBuf->copyVarLenData(val.data(), columnByteSize);
        } else if constexpr (db::IsList<T>) {
            // EntityList is only used as ColumnVector<EntityList>, never optional const
        } else if constexpr (db::IsNull<T>) {
            // Don't send anything for property null
        } else {
            static_assert(std::is_trivially_copyable_v<T>,
                          "TuringProtoEncoder only supports trivially copyable types and string");
            _outBuf->copyFixedLenData(&*opt, sizeof(T));
        }
    }

private:
    net::proto::TuringProtoOutBuf* _outBuf {nullptr};
};

class TuringProtoEncoder {
public:
    explicit TuringProtoEncoder(net::proto::TuringProtoOutBuf* outBuf);

    void writeDataframeHeader(const db::Dataframe* df);
    void writeDataframe(const db::Dataframe* df);
    void writeError(const db::QueryStatus* status);
    void writeProtocolError(std::string_view message);
    void writeEnd(db::QueryCallbacks::ExecTimeMilliseconds milliseconds);

private:
    net::proto::TuringProtoOutBuf* _outBuf {nullptr};
};

}
