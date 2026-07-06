#include "ParquetEdgeVisitor.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <type_traits>

#include <range/v3/view/zip.hpp>

#include <spdlog/fmt/fmt.h>

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "ID.h"
#include "datapart/EdgeRecord.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitBuilder.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;
namespace rv = ranges::views;

bool ParquetEdgeVisitor::onFileStart(const parquet::FileMetaData& metadata) {
    const parquet::SchemaDescriptor* schema = metadata.schema();
    const int numColumns = metadata.num_columns();

    for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
        const parquet::ColumnDescriptor* desc = schema->Column(columnIndex);
        const std::string path = desc->path()->ToDotString();
        const parquet::Type::type type = desc->physical_type();
        const int16_t maxDefLevel = desc->max_definition_level();

        if (path == SOURCE_COL_PATH) {
            const bool isInt64 = type == NODE_COL_TYPE;
            bioassert(isInt64, "Edge source column was not integral.");
            _srcColIdx = columnIndex;
        } else if (path == TARGET_COL_PATH) {
            const bool isInt64 = type == NODE_COL_TYPE;
            bioassert(isInt64, "Edge target column was not integral.");
            _tgtColIdx = columnIndex;
        } else if (path == EDGE_TYPE_COL_PATH) {
            const bool isString = type == EDGE_TYPE_COL_TYPE;
            bioassert(isString, "Edge type column was not string.");
            _edgetypeColIdx = columnIndex;
        } else {
            discoverPropertyColumn(columnIndex, path, type, maxDefLevel);
        }
    }

    if (_srcColIdx == INVALID_COL_IDX) {
        throw TuringException(
            fmt::format("Edge file missing {} column.", SOURCE_COL_PATH));
    }

    if (_tgtColIdx == INVALID_COL_IDX) {
        throw TuringException(
            fmt::format("Edge file missing {} column.", TARGET_COL_PATH));
    }

    if (_edgetypeColIdx == INVALID_COL_IDX) {
        throw TuringException(
            fmt::format("Edge file missing {} column.", EDGE_TYPE_COL_PATH));
    }

    return true;
}

bool ParquetEdgeVisitor::onRowGroupStart(size_t, const parquet::RowGroupMetaData&) {
    resetChunk();
    return true;
}

bool ParquetEdgeVisitor::onLevels(size_t columnIndex,
                                  std::span<const int16_t> repLevels,
                                  std::span<const int16_t> defLevels) {
    if (_propertyColumns.contains(columnIndex)) {
        capturePropertyLevels(columnIndex, defLevels);
    }
    return true;
}

bool ParquetEdgeVisitor::onInt64Values(size_t columnIndex, std::span<const int64_t> values) {
    if (columnIndex == _srcColIdx) {
        _chunkSrcIds = values;
    } else if (columnIndex == _tgtColIdx) {
        _chunkTgtIds = values;
    } else {
        capturePropertyInt64(columnIndex, values);
    }
    return true;
}

bool ParquetEdgeVisitor::onByteArrayValues(size_t columnIndex,
                                           std::span<const parquet::ByteArray> values) {
    if (columnIndex == _edgetypeColIdx) {
        fillEdgeTypes(values);
    } else {
        capturePropertyByteArray(columnIndex, values);
    }
    return true;
}

bool ParquetEdgeVisitor::onChunkEnd(size_t, size_t, size_t rows) {
    DataPartBuilder& builder = _builder->getCurrentBuilder();

    createEdges(&builder);
    applyEdgeProperties(rows);

    resetChunk();

    return true;
}

void ParquetEdgeVisitor::fillEdgeTypes(std::span<const parquet::ByteArray> types) {
    MetadataBuilder& metadataBuilder = _builder->metadata();

    for (const parquet::ByteArray& bytes : types) {
        const char* start = reinterpret_cast<const char*>(bytes.ptr);
        const size_t len = bytes.len;
        const std::string_view typeName {start, len};
        const EdgeTypeID typeID = metadataBuilder.getOrCreateEdgeType(typeName);

        _chunkEdgeTypes.push_back(typeID);
    }
}

void ParquetEdgeVisitor::createEdges(DataPartBuilder* builder) {
    bioassert(_chunkSrcIds.size() == _chunkTgtIds.size(), "Edge source/target mismatch");
    bioassert(_chunkSrcIds.size() == _chunkEdgeTypes.size(), "Edge, Type mismatch");

    for (auto [src, tgt, typeID] : rv::zip(_chunkSrcIds, _chunkTgtIds, _chunkEdgeTypes)) {
        const auto srcIt = _nodeIDs.find(src);
        bioassert(srcIt != end(_nodeIDs), "Missing source Node {}", src);

        const auto tgtIt = _nodeIDs.find(tgt);
        bioassert(tgtIt != end(_nodeIDs), "Missing target node {}", tgt);

        const NodeID srcID = srcIt->second;
        const NodeID tgtID = tgtIt->second;
        const EdgeRecord edgeRecord = builder->addEdge(typeID, srcID, tgtID);
        _chunkEdgeRecords.push_back(edgeRecord);
    }

    static_assert(std::is_trivially_copyable_v<EdgeRecord>);
}

void ParquetEdgeVisitor::applyEdgeProperties(size_t numRows) {
    bioassert(_chunkEdgeRecords.size() == numRows, "Edge count does not match chunk rows");

    for (const auto& [columnIndex, prop] : _propertyColumns) {
        const auto defLevelsIt = _propDefLevels.find(columnIndex);
        // Non-optional columns do not have an entry
        const bool haveDefLevels = defLevelsIt != end(_propDefLevels);

        const std::vector<int16_t>* defLevels = nullptr;
        if (haveDefLevels) {
            const std::vector<int16_t>& lvls = defLevelsIt->second;
            if (!lvls.empty()) {
                defLevels = &lvls;
            }
        }

        // Fast path: no def levels so every row has a property value
        if (!defLevels) {
            for (size_t row = 0; row < numRows; row++) {
                const EdgeRecord& edgeRecord = _chunkEdgeRecords[row];
                addEdgeProperty(edgeRecord, prop, columnIndex, row);
            }

            continue;
        }

        bioassert(defLevels->size() == numRows, "Definition levels, row mismatch.");
        // Def levels so nullable property, check every row for nullity
        size_t valueIndex = 0;
        for (size_t row = 0; row < numRows; ++row) {
            if ((*defLevels)[row] != prop.maxDefLevel) {
                continue; // null value at this row
            }

            const EdgeRecord& edgeRecord = _chunkEdgeRecords[row];
            addEdgeProperty(edgeRecord, prop, columnIndex, valueIndex);
            valueIndex++;
        }
    }
}

void ParquetEdgeVisitor::addEdgeProperty(const EdgeRecord& edge,
                                         const PropertyColumn& prop,
                                         size_t columnIndex,
                                         size_t valueIndex) {
    DataPartBuilder& builder = _builder->getCurrentBuilder();

    const ValueType valueType = prop.valueType;
    switch (valueType) {
        case ValueType::Int64: {
            const int64_t value = _propInt64Vals.at(columnIndex)[valueIndex];
            builder.addEdgeProperty<types::Int64>(edge, prop.propertyTypeID, value);
        }
        break;

        case ValueType::Double: {
            const double value = _propDoubleVals.at(columnIndex)[valueIndex];
            builder.addEdgeProperty<types::Double>(edge, prop.propertyTypeID, value);
        }
        break;

        case ValueType::Bool: {
            const CustomBool value = _propBoolVals.at(columnIndex)[valueIndex];
            builder.addEdgeProperty<types::Bool>(edge, prop.propertyTypeID, value);
        }
        break;

        case ValueType::String: {
            const parquet::ByteArray& bytes = _propByteArrayVals.at(columnIndex)[valueIndex];
            const std::string_view value(reinterpret_cast<const char*>(bytes.ptr),
                                         bytes.len);
            builder.addEdgeProperty<types::String>(edge, prop.propertyTypeID, value);
        }
        break;

        // ValueTypes that aren't represented in Parquet
        case ValueType::UInt64:
        case ValueType::Embedding:
        case ValueType::Invalid:
        case ValueType::_SIZE:
            throw FatalException(
                fmt::format("Unsupported type: {}.", ValueTypeName::value(valueType)));
        break;
    }
}

void ParquetEdgeVisitor::resetChunk() {
    _chunkSrcIds = {};
    _chunkTgtIds = {};
    _chunkEdgeTypes.clear();
    _chunkEdgeRecords.clear();
    resetPropertyChunk();
}
