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
#include <parquet/schema.h>
#include <parquet/types.h>

#include <spdlog/fmt/fmt.h>

#include "ParquetReader.h"

#include "PropertyContainerParquetLayout.h"
#include "ParquetMetadataParsing.h"

#include "properties/PropertyContainer.h"
#include "metadata/PropertyType.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = propertyContainerParquetLayout;

namespace {

// Streams each value-column batch straight into the matching TypedPropertyContainer<T> as
// it is read. The reader delivers row-aligned chunks with the entity-id column ahead of the
// value column, so every value batch lands after the ids it belongs to. Accumulating the
// whole value column first would transiently double the container's footprint, which on
// large property columns is tens of gigabytes on top of the resident graph.
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
            if (key == layout::VALUE_TYPE_KEY) {
                _valueType = static_cast<ValueType>(parseMetadataUint64(key, value));
                hasValueType = true;
            } else if (key == layout::DIMENSION_KEY) {
                _dimension = static_cast<size_t>(parseMetadataUint64(key, value));
            }
        }

        if (!hasValueType) {
            throw FatalException("PropertyContainerParquetLoader: missing value-type metadata");
        }

        const bool dimensionMissing = _valueType == ValueType::Embedding && _dimension == 0;
        if (dimensionMissing) {
            throw FatalException(
                "PropertyContainerParquetLoader: missing or zero embedding dimension");
        }

        checkSchema(metadata);

        switch (_valueType) {
            case ValueType::Int64:
                _container = std::make_unique<TypedPropertyContainer<types::Int64>>();
            break;
            case ValueType::UInt64:
                _container = std::make_unique<TypedPropertyContainer<types::UInt64>>();
            break;
            case ValueType::Double:
                _container = std::make_unique<TypedPropertyContainer<types::Double>>();
            break;
            case ValueType::Bool:
                _container = std::make_unique<TypedPropertyContainer<types::Bool>>();
            break;
            case ValueType::String:
                _container = std::make_unique<TypedPropertyContainer<types::String>>();
            break;
            case ValueType::Embedding:
                _container = std::make_unique<TypedPropertyContainer<types::Embedding>>(_dimension);
            break;
            case ValueType::Invalid:
            case ValueType::_SIZE:
                throw FatalException("PropertyContainerParquetLoader: invalid value type");
            break;
        }

        return true;
    }

    bool onInt64Values(size_t columnIndex, std::span<const int64_t> values) override {
        if (columnIndex == 0) {
            for (const int64_t value : values) {
                _entityIds.push_back(value);
            }
            return true;
        }

        if (_valueType == ValueType::Int64) {
            addValueBatch<TypedPropertyContainer<types::Int64>>(
                values, [](int64_t value) { return value; });
        } else {
            addValueBatch<TypedPropertyContainer<types::UInt64>>(
                values, [](int64_t value) { return static_cast<uint64_t>(value); });
        }
        return true;
    }

    bool onDoubleValues(size_t columnIndex, std::span<const double> values) override {
        addValueBatch<TypedPropertyContainer<types::Double>>(
            values, [](double value) { return value; });
        return true;
    }

    bool onBoolValues(size_t columnIndex, std::span<const bool> values) override {
        addValueBatch<TypedPropertyContainer<types::Bool>>(
            values, [](bool value) { return value; });
        return true;
    }

    bool onByteArrayValues(size_t columnIndex,
                           std::span<const parquet::ByteArray> values) override {
        addValueBatch<TypedPropertyContainer<types::String>>(
            values, [](const parquet::ByteArray& byteArray) {
                return std::string_view(reinterpret_cast<const char*>(byteArray.ptr), byteArray.len);
            });
        return true;
    }

    bool onFixedLenByteArrayValues(size_t columnIndex,
                                   std::span<const parquet::FixedLenByteArray> values,
                                   size_t byteWidth) override {
        const size_t floatsPerValue = byteWidth / sizeof(float);

        addValueBatch<TypedPropertyContainer<types::Embedding>>(
            values, [floatsPerValue](const parquet::FixedLenByteArray& value) {
                return std::span<const float>(reinterpret_cast<const float*>(value.ptr), floatsPerValue);
            });
        return true;
    }

    std::unique_ptr<PropertyContainer> build() {
        if (_valueRowsLoaded != _entityIds.size()) {
            throw FatalException(fmt::format(
                "PropertyContainerParquetLoader: {} values for {} entity ids",
                _valueRowsLoaded, _entityIds.size()));
        }

        return std::move(_container);
    }

private:
    ValueType _valueType {ValueType::Invalid};
    size_t _dimension {0};
    std::vector<int64_t> _entityIds;
    std::unique_ptr<PropertyContainer> _container;
    size_t _valueRowsLoaded {0};

    // Streams one value-column batch into the typed container. ParquetValue is the element
    // type the reader delivers; convert maps it to the container's value (identity for the
    // scalars, a view for strings, a float span for embeddings — the container copies in).
    template <typename ContainerType, typename ParquetValue, typename Convert>
    void addValueBatch(std::span<const ParquetValue> values, Convert convert) {
        if (_valueRowsLoaded + values.size() > _entityIds.size()) {
            throw FatalException(fmt::format(
                "PropertyContainerParquetLoader: value column runs ahead of entity ids"
                " ({} loaded + {} new for {} ids)",
                _valueRowsLoaded, values.size(), _entityIds.size()));
        }

        auto* container = static_cast<ContainerType*>(_container.get());
        for (const ParquetValue& value : values) {
            container->add(entityIdAt(_valueRowsLoaded), convert(value));
            ++_valueRowsLoaded;
        }
    }

    parquet::Type::type valuePhysicalType() const {
        switch (_valueType) {
            case ValueType::Int64:
            case ValueType::UInt64:
                return parquet::Type::INT64;
            break;
            case ValueType::Double:
                return parquet::Type::DOUBLE;
            break;
            case ValueType::Bool:
                return parquet::Type::BOOLEAN;
            break;
            case ValueType::String:
                return parquet::Type::BYTE_ARRAY;
            break;
            case ValueType::Embedding:
                return parquet::Type::FIXED_LEN_BYTE_ARRAY;
            break;
            case ValueType::Invalid:
            case ValueType::_SIZE:
                throw FatalException("PropertyContainerParquetLoader: invalid value type");
            break;
        }

        throw FatalException("PropertyContainerParquetLoader: unhandled value type");
    }

    // The value column's physical type depends on the value-type metadata, so the schema
    // can only be checked here, after onFileStart has read it — not up front through
    // ParquetReader::setExpectedSchema like the fixed-schema loaders.
    void checkSchema(const parquet::FileMetaData& metadata) const {
        if (metadata.num_columns() != 2) {
            throw FatalException(fmt::format(
                "PropertyContainerParquetLoader: expected 2 columns, file has {}",
                metadata.num_columns()));
        }

        const parquet::SchemaDescriptor* schema = metadata.schema();

        const parquet::ColumnDescriptor* entityColumn = schema->Column(0);
        const bool entityColumnMatches = entityColumn->name() == layout::ENTITY_ID_COLUMN
                                         && entityColumn->physical_type() == parquet::Type::INT64;
        if (!entityColumnMatches) {
            throw FatalException("PropertyContainerParquetLoader: unexpected entity-id column");
        }

        const parquet::ColumnDescriptor* valueColumn = schema->Column(1);
        const bool valueColumnMatches = valueColumn->name() == layout::VALUE_COLUMN
                                        && valueColumn->physical_type() == valuePhysicalType();
        if (!valueColumnMatches) {
            throw FatalException(fmt::format(
                "PropertyContainerParquetLoader: value column does not match value type {}",
                static_cast<unsigned>(_valueType)));
        }

        const bool isEmbedding = _valueType == ValueType::Embedding;
        if (isEmbedding) {
            const size_t expectedByteWidth = _dimension * sizeof(float);
            const size_t fileByteWidth = static_cast<size_t>(valueColumn->type_length());
            if (fileByteWidth != expectedByteWidth) {
                throw FatalException(fmt::format(
                    "PropertyContainerParquetLoader: embedding column byte width {} does not"
                    " match dimension {}",
                    fileByteWidth, _dimension));
            }
        }
    }

    EntityID entityIdAt(size_t index) const {
        return EntityID {static_cast<uint64_t>(_entityIds[index])};
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
