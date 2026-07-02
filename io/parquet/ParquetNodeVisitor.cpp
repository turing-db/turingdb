#include "ParquetNodeVisitor.h"

#include <cstdint>
#include <string>
#include <string_view>

#include <range/v3/view/zip.hpp>

#include <spdlog/fmt/fmt.h>

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "ID.h"
#include "TuringException.h"
#include "metadata/LabelSet.h"
#include "metadata/PropertyType.h"
#include "versioning/CommitBuilder.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;
namespace rv = ranges::views;

bool ParquetNodeVisitor::onFileStart(const parquet::FileMetaData& metadata) {
    const parquet::SchemaDescriptor* schema = metadata.schema();
    const int numColumns = metadata.num_columns();

    for (int columnIndex = 0; columnIndex < numColumns; columnIndex++) {
        const parquet::ColumnDescriptor* desc = schema->Column(columnIndex);
        const std::string path = desc->path()->ToDotString();
        const parquet::Type::type type = desc->physical_type();
        const int16_t maxDefLevel = desc->max_definition_level();

        if (path == NODE_COL_PATH) {
            const bool isInt64 = type == NODE_COL_TYPE;
            bioassert(isInt64, "Node column was not integral.");
            _nodeColIdx = columnIndex;
            _nodeIdMaxDefLevel = maxDefLevel;
        } else if (path == LABELS_COL_PATH) {
            const bool isLabels = type == LABELS_COL_TYPE;
            bioassert(isLabels, "Labels column was not a byte array.");
            _lblColIdx = columnIndex;
            _lblMaxDefLevel = maxDefLevel;
        } else {
            discoverPropertyColumn(columnIndex, path, type, maxDefLevel);
        }
    }

    if (_nodeColIdx == INVALID_COL_IDX) {
        throw TuringException(
            fmt::format("Node file missing {} column.", NODE_COL_PATH));
    }

    if (_lblColIdx == INVALID_COL_IDX) {
        throw TuringException(
            fmt::format("Node file missing {} column.", LABELS_COL_PATH));
    }

    return true;
}

bool ParquetNodeVisitor::onRowGroupStart(size_t, const parquet::RowGroupMetaData&) {
    resetChunk();
    return true;
}

bool ParquetNodeVisitor::onLevels(size_t columnIndex,
                                  std::span<const int16_t> repLevels,
                                  std::span<const int16_t> defLevels) {
    if (columnIndex == _lblColIdx) {
        _chunkLabelRepLevels = repLevels;
        _chunkLabelDefLevels = defLevels;
    } else if (columnIndex == _nodeColIdx) {
        _chunkNodeIdDefLevels.insert(end(_chunkNodeIdDefLevels), begin(defLevels), end(defLevels));
    } else {
        capturePropertyLevels(columnIndex, defLevels);
    }
    return true;
}

bool ParquetNodeVisitor::onInt64Values(size_t columnIndex, std::span<const int64_t> values) {
    if (columnIndex == _nodeColIdx) {
        _chunkNodeIds = values;
    } else {
        capturePropertyInt64(columnIndex, values);
    }
    return true;
}

bool ParquetNodeVisitor::onByteArrayValues(size_t columnIndex,
                                           std::span<const parquet::ByteArray> values) {
    if (columnIndex == _lblColIdx) {
        fillLabels(values);
    } else {
        capturePropertyByteArray(columnIndex, values);
    }
    return true;
}

bool ParquetNodeVisitor::onChunkEnd(size_t, size_t, size_t) {
    DataPartBuilder& builder = _builder->getCurrentBuilder();

    createNodes(&builder);
    applyNodeProperties();

    resetChunk();

    return true;
}

void ParquetNodeVisitor::fillLabels(std::span<const parquet::ByteArray> labels) {
    bioassert(_chunkLabelRepLevels.size() == _chunkLabelDefLevels.size(),
              "Labels: rep and def level counts differ");

    MetadataBuilder& metadataBuilder = _builder->metadata();
    size_t valueIndex = 0;

    for (size_t i = 0; i < _chunkLabelRepLevels.size(); ++i) {
        const bool hasValue = _chunkLabelDefLevels[i] == _lblMaxDefLevel;

        if (!hasValue) {
            continue;
        }

        const bool nextNode = _chunkLabelRepLevels[i] == 0;
        if (nextNode) {
            _chunkNodeLabels.emplace_back();
        }

        const parquet::ByteArray& bytes = labels[valueIndex++];
        const char* start = reinterpret_cast<const char*>(bytes.ptr);
        const size_t len = bytes.len;
        const std::string_view labelName {start, len};
        const LabelID labelID = metadataBuilder.getOrCreateLabel(labelName);
        _chunkNodeLabels.back().push_back(labelID);
    }

    bioassert(valueIndex == labels.size(), "Labels: not all values were consumed");
}

void ParquetNodeVisitor::createNodes(DataPartBuilder* builder) {
    bioassert(_chunkNodeIds.size() == _chunkNodeLabels.size(), "NodeID, Label mismatch");

    for (auto [id, labelIDs] : rv::zip(_chunkNodeIds, _chunkNodeLabels)) {
        LabelSet labelSet;
        for (const LabelID labelID : labelIDs) {
            labelSet.set(labelID);
        }
        const NodeID assignedID = builder->addNode(labelSet);
        _nodeIDs[id] = assignedID;
    }
}

void ParquetNodeVisitor::applyNodeProperties() {
    const size_t numRows = _chunkNodeIdDefLevels.size();

    for (const auto& [columnIndex, prop] : _propertyColumns) {
        const auto defLevelsIt = _propDefLevels.find(columnIndex);
        if (defLevelsIt == end(_propDefLevels) || defLevelsIt->second.empty()) {
            continue;
        }

        const std::vector<int16_t>& defLevels = defLevelsIt->second;
        size_t valueIndex = 0;

        for (size_t row = 0; row < numRows; ++row) {
            const bool hasValue = defLevels[row] == prop.maxDefLevel;
            if (!hasValue) {
                continue;
            }

            const NodeID nodeID = _nodeIDs.at(_chunkNodeIds[row]);
            addNodeProperty(nodeID, prop, columnIndex, valueIndex);
            valueIndex++;
        }
    }
}

void ParquetNodeVisitor::addNodeProperty(NodeID id,
                                         const PropertyColumn& prop,
                                         size_t columnIndex,
                                         size_t valueIndex) {
    DataPartBuilder& builder = _builder->getCurrentBuilder();

    const ValueType valueType = prop.valueType;
    switch (valueType) {
        case ValueType::Int64: {
            const int64_t value = _propInt64Vals.at(columnIndex)[valueIndex];
            builder.addNodeProperty<types::Int64>(id, prop.propertyTypeID, value);
        }
        break;

        case ValueType::Double: {
            const double value = _propDoubleVals.at(columnIndex)[valueIndex];
            builder.addNodeProperty<types::Double>(id, prop.propertyTypeID, value);
        }
        break;

        case ValueType::Bool: {
            const CustomBool value = _propBoolVals.at(columnIndex)[valueIndex];
            builder.addNodeProperty<types::Bool>(id, prop.propertyTypeID, value);
        }
        break;

        case ValueType::String: {
            const parquet::ByteArray& bytes = _propByteArrayVals.at(columnIndex)[valueIndex];
            const std::string_view value(reinterpret_cast<const char*>(bytes.ptr),
                                         bytes.len);
            builder.addNodeProperty<types::String>(id, prop.propertyTypeID, value);
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

void ParquetNodeVisitor::resetChunk() {
    _chunkNodeIds = {};
    _chunkNodeLabels.clear();
    _chunkNodeIdDefLevels.clear();
    _chunkLabelRepLevels = {};
    _chunkLabelDefLevels = {};
    resetPropertyChunk();
}
