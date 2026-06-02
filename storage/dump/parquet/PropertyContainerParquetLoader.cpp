#include "PropertyContainerParquetLoader.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <arrow/util/key_value_metadata.h>
#include <parquet/metadata.h>
#include <parquet/types.h>

#include "ParquetReader.h"

#include "properties/PropertyContainer.h"
#include "metadata/PropertyType.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace {

constexpr std::string_view VALUE_TYPE_KEY = "turing.value_type";
constexpr std::string_view DIMENSION_KEY = "turing.embedding_dimension";

// Collects the entity-id column and the typed value column across all chunks, then
// rebuilds the matching TypedPropertyContainer<T> in build().
class PropertyContainerLoadVisitor : public ParquetSaxVisitor {
public:
    bool onFileStart(const parquet::FileMetaData& metadata) override {
        const auto& keyValueMetadata = metadata.key_value_metadata();
        if (!keyValueMetadata) {
            throw FatalException("PropertyContainerParquetLoader: file has no key/value metadata");
        }

        bool hasValueType = false;
        for (int64_t i = 0; i < keyValueMetadata->size(); ++i) {
            const std::string& key = keyValueMetadata->key(i);
            const std::string& value = keyValueMetadata->value(i);
            if (key == VALUE_TYPE_KEY) {
                _valueType = static_cast<ValueType>(std::stoul(value));
                hasValueType = true;
            } else if (key == DIMENSION_KEY) {
                _dimension = static_cast<size_t>(std::stoul(value));
            }
        }

        if (!hasValueType) {
            throw FatalException("PropertyContainerParquetLoader: missing value-type metadata");
        }

        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        std::vector<int64_t>& target = (columnIndex == 0) ? _entityIds : _int64Values;
        for (const int64_t value : values) {
            target.push_back(value);
        }
        return true;
    }

    bool onDoubleValues(size_t columnIndex, std::span<const double> values) override {
        for (const double value : values) {
            _doubleValues.push_back(value);
        }
        return true;
    }

    bool onBoolValues(size_t columnIndex, std::span<const bool> values) override {
        for (const bool value : values) {
            _boolValues.push_back(value ? 1 : 0);
        }
        return true;
    }

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override {
        for (const parquet::ByteArray& byteArray : values) {
            _stringValues.emplace_back(reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
        }
        return true;
    }

    bool onFixedLenByteArrayValues(size_t columnIndex,
                                   std::span<const parquet::FixedLenByteArray> values,
                                   size_t byteWidth) override {
        const size_t floatsPerValue = byteWidth / sizeof(float);
        for (const parquet::FixedLenByteArray& value : values) {
            const float* floats = reinterpret_cast<const float*>(value.ptr);
            for (size_t i = 0; i < floatsPerValue; ++i) {
                _embeddingFloats.push_back(floats[i]);
            }
        }
        return true;
    }

    std::unique_ptr<PropertyContainer> build() const {
        switch (_valueType) {
            case ValueType::Int64:
                return buildInt64();
            break;
            case ValueType::UInt64:
                return buildUInt64();
            break;
            case ValueType::Double:
                return buildDouble();
            break;
            case ValueType::Bool:
                return buildBool();
            break;
            case ValueType::String:
                return buildString();
            break;
            case ValueType::Embedding:
                return buildEmbedding();
            break;
            case ValueType::Invalid:
            case ValueType::_SIZE:
                throw FatalException("PropertyContainerParquetLoader: invalid value type");
            break;
        }

        throw FatalException("PropertyContainerParquetLoader: unhandled value type");
    }

private:
    ValueType _valueType {ValueType::Invalid};
    size_t _dimension {0};
    std::vector<int64_t> _entityIds;
    std::vector<int64_t> _int64Values;
    std::vector<double> _doubleValues;
    std::vector<uint8_t> _boolValues;
    std::vector<std::string> _stringValues;
    std::vector<float> _embeddingFloats;

    EntityID entityIdAt(size_t index) const {
        return EntityID {static_cast<uint64_t>(_entityIds[index])};
    }

    std::unique_ptr<PropertyContainer> buildInt64() const {
        auto container = std::make_unique<TypedPropertyContainer<types::Int64>>();
        for (size_t i = 0; i < _entityIds.size(); ++i) {
            container->add(entityIdAt(i), _int64Values[i]);
        }
        return container;
    }

    std::unique_ptr<PropertyContainer> buildUInt64() const {
        auto container = std::make_unique<TypedPropertyContainer<types::UInt64>>();
        for (size_t i = 0; i < _entityIds.size(); ++i) {
            container->add(entityIdAt(i), static_cast<uint64_t>(_int64Values[i]));
        }
        return container;
    }

    std::unique_ptr<PropertyContainer> buildDouble() const {
        auto container = std::make_unique<TypedPropertyContainer<types::Double>>();
        for (size_t i = 0; i < _entityIds.size(); ++i) {
            container->add(entityIdAt(i), _doubleValues[i]);
        }
        return container;
    }

    std::unique_ptr<PropertyContainer> buildBool() const {
        auto container = std::make_unique<TypedPropertyContainer<types::Bool>>();
        for (size_t i = 0; i < _entityIds.size(); ++i) {
            container->add(entityIdAt(i), static_cast<bool>(_boolValues[i] != 0));
        }
        return container;
    }

    std::unique_ptr<PropertyContainer> buildString() const {
        auto container = std::make_unique<TypedPropertyContainer<types::String>>();
        for (size_t i = 0; i < _entityIds.size(); ++i) {
            container->add(entityIdAt(i), std::string_view(_stringValues[i]));
        }
        return container;
    }

    std::unique_ptr<PropertyContainer> buildEmbedding() const {
        auto container = std::make_unique<TypedPropertyContainer<types::Embedding>>(_dimension);
        for (size_t i = 0; i < _entityIds.size(); ++i) {
            const std::span<const float> view(_embeddingFloats.data() + i * _dimension, _dimension);
            container->add(entityIdAt(i), view);
        }
        return container;
    }
};

}

std::unique_ptr<PropertyContainer> PropertyContainerParquetLoader::load(const fs::Path& path) {
    PropertyContainerLoadVisitor visitor;
    ParquetReader reader(path, visitor);
    while (reader.nextChunk()) {
    }
    return visitor.build();
}
