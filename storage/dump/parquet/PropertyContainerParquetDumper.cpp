#include "PropertyContainerParquetDumper.h"

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <span>
#include <string_view>
#include <vector>

#include <spdlog/fmt/fmt.h>

#include "ParquetWriteSchema.h"
#include "ParquetWriter.h"

#include "PropertyContainerParquetLayout.h"

#include "properties/PropertyContainer.h"
#include "metadata/PropertyType.h"
#include "Path.h"

#include "FatalException.h"

using namespace db;

namespace layout = propertyContainerParquetLayout;

namespace {

void gatherEntityIds(const PropertyContainer& props, std::vector<int64_t>& idsOut) {
    const PropertyContainer::IDs& ids = props.ids();
    idsOut.clear();
    idsOut.reserve(ids.size());
    for (const EntityID id : ids) {
        idsOut.push_back(static_cast<int64_t>(id.getValue()));
    }
}

}

void PropertyContainerParquetDumper::dump(const PropertyContainer& props, const fs::Path& path) {
    const ValueType valueType = props.getValueType();
    const size_t count = props.size();

    size_t embeddingDimension = 0;

    ParquetWriteSchema schema;
    schema.addColumn(layout::ENTITY_ID_COLUMN, ParquetColumnType::UInt64);

    switch (valueType) {
        case ValueType::Int64:
            schema.addColumn(layout::VALUE_COLUMN, ParquetColumnType::Int64);
        break;
        case ValueType::UInt64:
            schema.addColumn(layout::VALUE_COLUMN, ParquetColumnType::UInt64);
        break;
        case ValueType::Double:
            schema.addColumn(layout::VALUE_COLUMN, ParquetColumnType::Double);
        break;
        case ValueType::Bool:
            schema.addColumn(layout::VALUE_COLUMN, ParquetColumnType::Bool);
        break;
        case ValueType::String:
            schema.addColumn(layout::VALUE_COLUMN, ParquetColumnType::String);
        break;
        case ValueType::Embedding:
            embeddingDimension = props.cast<types::Embedding>().getRawContainer().getDimension();
            schema.addFixedLenColumn(layout::VALUE_COLUMN, embeddingDimension * sizeof(float));
        break;
        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw FatalException("PropertyContainerParquetDumper: invalid property value type");
        break;
    }

    ParquetWriter writer(path, schema);
    writer.setMetadata(layout::VALUE_TYPE_KEY, fmt::format("{}", static_cast<unsigned>(valueType)));
    if (valueType == ValueType::Embedding) {
        writer.setMetadata(layout::DIMENSION_KEY, fmt::format("{}", embeddingDimension));
    }

    if (count > 0) {
        writer.beginRowGroup(count);

        std::vector<int64_t> entityIds;
        gatherEntityIds(props, entityIds);
        writer.writeInt64Column(0, entityIds);

        switch (valueType) {
            case ValueType::Int64:
                writer.writeInt64Column(1, props.cast<types::Int64>().all());
            break;
            case ValueType::UInt64: {
                const std::span<const uint64_t> values = props.cast<types::UInt64>().all();
                const std::span<const int64_t> asInt64(
                    reinterpret_cast<const int64_t*>(values.data()), values.size());
                writer.writeInt64Column(1, asInt64);
            }
            break;
            case ValueType::Double:
                writer.writeDoubleColumn(1, props.cast<types::Double>().all());
            break;
            case ValueType::Bool: {
                const std::span<const CustomBool> values = props.cast<types::Bool>().all();
                const std::unique_ptr<bool[]> boolBuffer(new bool[count]);
                for (size_t i = 0; i < count; ++i) {
                    boolBuffer[i] = static_cast<bool>(values[i]);
                }
                writer.writeBoolColumn(1, std::span<const bool>(boolBuffer.get(), count));
            }
            break;
            case ValueType::String:
                writer.writeStringColumn(1, props.cast<types::String>().all());
            break;
            case ValueType::Embedding: {
                const std::span<const types::Embedding::Primitive> views =
                    props.cast<types::Embedding>().all();
                std::vector<float> flat;
                flat.reserve(count * embeddingDimension);
                for (const types::Embedding::Primitive view : views) {
                    flat.insert(flat.end(), view.begin(), view.end());
                }
                writer.writeFixedLenColumn(1,
                                           std::span<const float>(flat.data(), flat.size()),
                                           embeddingDimension * sizeof(float));
            }
            break;
            case ValueType::Invalid:
            case ValueType::_SIZE:
                throw FatalException("PropertyContainerParquetDumper: invalid property value type");
            break;
        }
    }

    writer.finish();
}
