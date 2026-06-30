#include "ParquetNeo4jVisitor.h"

#include <cstdint>
#include <string_view>

#include <range/v3/view/zip.hpp>

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "BioAssert.h"

#include "FatalException.h"
#include "ID.h"
#include "metadata/PropertyType.h"
#include "spdlog/spdlog.h"
#include "versioning/CommitBuilder.h"
#include "writers/DataPartBuilder.h"
#include "writers/MetadataBuilder.h"
#include "metadata/LabelSet.h"

using namespace db;
namespace rv = ranges::views;

void ParquetNeo4jVisitor::fillLabels(std::span<const parquet::ByteArray> labels) {
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

void ParquetNeo4jVisitor::fillEdgeTypes(std::span<const parquet::ByteArray> types) {
    MetadataBuilder& metadataBuilder = _builder->metadata();

    for (const parquet::ByteArray& bytes : types) {
        const char* start = reinterpret_cast<const char*>(bytes.ptr);
        const size_t len = bytes.len;
        const std::string_view typeName {start, len};
        const EdgeTypeID typeID = metadataBuilder.getOrCreateEdgeType(typeName);

        _chunkEdgeTypes.push_back(typeID);
    }
}

void ParquetNeo4jVisitor::applyProperties() {
    DataPartBuilder& dpBuilder = _builder->getCurrentBuilder();

    // A row may contain a node or edge definition. A node row will have an entry in the
    // __id column, whilst an edge row will have an entry in the __src column. Determine
    // which rows are nodes, and which are edges, so that property columns are applied to
    // the correct entities.
    const size_t numRows = _chunkNodeIdDefLevels.size();
    std::vector<bool> isNodeRow(numRows, false);
    std::vector<bool> isEdgeRow(numRows, false);
    for (size_t row = 0; row < numRows; ++row) {
        isNodeRow[row] = _chunkNodeIdDefLevels[row] == _nodeIdMaxDefLevel;
        isEdgeRow[row] = _chunkSrcIdDefLevels[row] == _srcIdMaxDefLevel;
    }

    for (const auto& [colIdx, prop] : _propertyColumns) {
        const auto defLevelsIt = _propDefLevels.find(colIdx);
        if (defLevelsIt == end(_propDefLevels) || defLevelsIt->second.empty()) {
            continue;
        }

        const std::vector<int16_t>& defLevels = defLevelsIt->second;
        size_t nodeIdx = 0;
        size_t edgeIdx = 0;
        size_t valIdx = 0;

        for (size_t row = 0; row < numRows; ++row) {
            const bool hasVal = defLevels[row] == prop.maxDefLevel;

            if (!hasVal) {
                if (isNodeRow[row]) {
                    ++nodeIdx;
                } else if (isEdgeRow[row]) {
                    ++edgeIdx;
                }
                continue;
            }
            if (isNodeRow[row]) {
                const NodeID nodeID = _nodeIDs.at(_chunkNodeIds[nodeIdx]);

                switch (prop.valueType) {
                    case ValueType::Int64: {
                        const int64_t value = _propInt64Vals.at(colIdx)[valIdx];
                        dpBuilder.addNodeProperty<types::Int64>(nodeID, prop.propertyTypeID, value);
                    }
                    break;

                    case ValueType::Double: {
                        const double value = _propDoubleVals.at(colIdx)[valIdx];
                        dpBuilder.addNodeProperty<types::Double>(nodeID, prop.propertyTypeID, value);
                    }
                    break;

                    case ValueType::Bool: {
                        const CustomBool value = _propBoolVals.at(colIdx)[valIdx];
                        dpBuilder.addNodeProperty<types::Bool>(nodeID, prop.propertyTypeID, value);
                    }
                    break;

                    case ValueType::String: {
                        const parquet::ByteArray& bytes = _propByteArrayVals.at(colIdx)[valIdx];
                        const std::string_view value(reinterpret_cast<const char*>(bytes.ptr), bytes.len);
                        dpBuilder.addNodeProperty<types::String>(nodeID, prop.propertyTypeID, value);
                    }
                    break;

                    case ValueType::UInt64:
                    case ValueType::Embedding:
                    case ValueType::Invalid:
                    case ValueType::_SIZE:
                        throw FatalException(fmt::format("Unsupported type: {}.",
                            ValueTypeName::value(prop.valueType)));
                    break;
                }
                ++nodeIdx;
            } else if (isEdgeRow[row]) {
                const EdgeRecord& edgeRecord = _chunkEdgeRecords[edgeIdx];

                switch (prop.valueType) {
                    case ValueType::Int64: {
                        const int64_t value = _propInt64Vals.at(colIdx)[valIdx];
                        dpBuilder.addEdgeProperty<types::Int64>(edgeRecord, prop.propertyTypeID, value);
                    }
                    break;

                    case ValueType::Double: {
                        const double value = _propDoubleVals.at(colIdx)[valIdx];
                        dpBuilder.addEdgeProperty<types::Double>(edgeRecord, prop.propertyTypeID, value);
                    }
                    break;

                    case ValueType::Bool: {
                        const CustomBool value = _propBoolVals.at(colIdx)[valIdx];
                        dpBuilder.addEdgeProperty<types::Bool>(edgeRecord, prop.propertyTypeID, value);
                    }
                    break;

                    case ValueType::String: {
                        const parquet::ByteArray& bytes = _propByteArrayVals.at(colIdx)[valIdx];
                        const std::string_view value(reinterpret_cast<const char*>(bytes.ptr), bytes.len);
                        dpBuilder.addEdgeProperty<types::String>(edgeRecord, prop.propertyTypeID, value);
                    }
                    break;

                    case ValueType::UInt64:
                    case ValueType::Embedding:
                    case ValueType::Invalid:
                    case ValueType::_SIZE:
                        throw FatalException(fmt::format("Unsupported type: {}.",
                            ValueTypeName::value(prop.valueType)));
                    break;
                }
                ++edgeIdx;
            }
            ++valIdx;
        }
    }
}

bool ParquetNeo4jVisitor::onFileStart(const parquet::FileMetaData& metadata) {
    const parquet::SchemaDescriptor* sch = metadata.schema();
    const int numCols = metadata.num_columns();
    MetadataBuilder& metadataBuilder = _builder->metadata();

    for (int colIdx = 0; colIdx < numCols; colIdx++) {
        const parquet::ColumnDescriptor* desc = sch->Column(colIdx);
        const std::shared_ptr<parquet::schema::ColumnPath> colPath = desc->path();
        const std::string path = colPath->ToDotString();
        const parquet::Type::type type = desc->physical_type();

        if (path == NEO4J_NODE_COL_PATH) {
            const bool isInt64 = type == NEO4J_NODE_COL_TYPE;
            bioassert(isInt64, "Neo4j node column was not integral.");
            _nodeColIdx = colIdx;
            _nodeIdMaxDefLevel = desc->max_definition_level();
            continue;
        }

        if (path == NEO4J_LBLS_COL_PATH) {
            const bool isLists = type == NEO4J_LBLS_COL_TYPE;
            bioassert(isLists, "Neo4j labels column was not a byte array.");
            _lblColIdx = colIdx;
            _lblMaxDefLevel = desc->max_definition_level();
            continue;
        }

        if (path == NEO4J_SRC_COL_PATH) {
            const bool isInt64 = type == NEO4J_NODE_COL_TYPE;
            bioassert(isInt64, "Neo4j edge source column was not integral.");
            _srcColIdx = colIdx;
            _srcIdMaxDefLevel = desc->max_definition_level();
            continue;
        }

        if (path == NEO4J_TGT_COL_PATH) {
            const bool isInt64 = type == NEO4J_NODE_COL_TYPE;
            bioassert(isInt64, "Neo4j edge target column was not integral.");
            _tgtColIdx = colIdx;
            continue;
        }

        if (path == NEO4J_ETYPE_COL_PATH) {
            const bool isString = type == NEO4J_ETYPE_COL_TYPE;
            bioassert(isString, "Neo4j edge type column was not string.");
            _edgetypeColIdx = colIdx;
            continue;
        }

        ValueType valueType = ValueType::Invalid;
        // FIXME: Byte arrays always strings. Check for lists, etc.
        switch (type) {
            case parquet::Type::INT64:
                valueType = ValueType::Int64;
            break;
            case parquet::Type::BYTE_ARRAY:
                valueType = ValueType::String;
            break;
            case parquet::Type::BOOLEAN:
                valueType = ValueType::Bool;
            break;
            case parquet::Type::DOUBLE:
                valueType = ValueType::Double;
            break;
            default:
                spdlog::warn("Parquet property column '{}': unsupported physical type "
                             "{}, skipping",
                             path, static_cast<int>(type));
                continue;
            break;
        }

        const PropertyType propType = metadataBuilder.getOrCreatePropertyType(path, valueType);
        const size_t columnIndex = colIdx;

        PropertyColumn col {.name = path,
                            .valueType = valueType,
                            .propertyTypeID = propType._id,
                            .maxDefLevel = desc->max_definition_level()};

        _propertyColumns[columnIndex] = std::move(col);
    }

    return true;
}

bool ParquetNeo4jVisitor::onRowGroupStart(size_t, const parquet::RowGroupMetaData&) {
    _chunkNodeIds = {};
    _chunkNodeLabels.clear();
    _chunkEdgeRecords.clear();
    _chunkNodeIdDefLevels.clear();
    _chunkSrcIdDefLevels.clear();
    for (auto& [colIdx, levels] : _propDefLevels) {
        levels.clear();
    }
    return true;
}

bool ParquetNeo4jVisitor::onLevels(size_t columnIndex,
                                   std::span<const int16_t> repLevels,
                                   std::span<const int16_t> defLevels) {
    if (columnIndex == _lblColIdx) {
        _chunkLabelRepLevels = repLevels;
        _chunkLabelDefLevels = defLevels;
    } else if (columnIndex == _nodeColIdx) {
        _chunkNodeIdDefLevels.insert(end(_chunkNodeIdDefLevels), begin(defLevels), end(defLevels));
    } else if (columnIndex == _srcColIdx) {
        _chunkSrcIdDefLevels.insert(end(_chunkSrcIdDefLevels), begin(defLevels), end(defLevels));
    } else if (_propertyColumns.contains(columnIndex)) {
        _propDefLevels[columnIndex].insert(
            end(_propDefLevels[columnIndex]), begin(defLevels), end(defLevels));
    }
    return true;
}

bool ParquetNeo4jVisitor::onInt32Values(size_t columnIndex, std::span<const int32_t> values) {
    return true;
}

bool ParquetNeo4jVisitor::onInt64Values(size_t columnIndex, std::span<const int64_t> values) {
    if (columnIndex == _nodeColIdx) {
        _chunkNodeIds = values;
    } else if (columnIndex == _srcColIdx) {
        _chunkSrcIds = values;
    } else if (columnIndex == _tgtColIdx) {
        _chunkTgtIds = values;
    } else if (_propertyColumns.contains(columnIndex)) {
        _propInt64Vals[columnIndex] = values;
    }
    return true;
}

bool ParquetNeo4jVisitor::onDoubleValues(size_t columnIndex, std::span<const double> values) {
    if (_propertyColumns.contains(columnIndex)) {
        _propDoubleVals[columnIndex] = values;
    }
    return true;
}

bool ParquetNeo4jVisitor::onBoolValues(size_t columnIndex, std::span<const bool> values) {
    if (_propertyColumns.contains(columnIndex)) {
        _propBoolVals[columnIndex] = values;
    }
    return true;
}

bool ParquetNeo4jVisitor::onByteArrayValues(size_t columnIndex,
                                            std::span<const parquet::ByteArray> values) {
    if (columnIndex == _lblColIdx) {
        fillLabels(values);
    } else if (columnIndex == _edgetypeColIdx) {
        fillEdgeTypes(values);
    } else if (_propertyColumns.contains(columnIndex)) {
        _propByteArrayVals[columnIndex] = values;
    }
    return true;
}

bool ParquetNeo4jVisitor::onChunkEnd(size_t, size_t, size_t) {
    DataPartBuilder& dpBuilder = _builder->getCurrentBuilder();

    // Build nodes
    bioassert(_chunkNodeIds.size() == _chunkNodeLabels.size(), "NodeID, Label mismatch");
    for (auto [id, labelIDs] : rv::zip(_chunkNodeIds, _chunkNodeLabels)) {
        LabelSet labelSet;
        for (const LabelID labelID : labelIDs) {
            labelSet.set(labelID);
        }
        const NodeID assignedID = dpBuilder.addNode(labelSet);
        _nodeIDs[id] = assignedID;
    }
    _chunkNodeLabels.clear();

    // Build edges, saving each EdgeRecord for property application below
    bioassert(_chunkSrcIds.size() == _chunkTgtIds.size(), "Edge source/target mismatch");
    bioassert(_chunkSrcIds.size() == _chunkEdgeTypes.size(), "Edge, Type mismatch");
    for (auto [src, tgt, typeID] : rv::zip(_chunkSrcIds, _chunkTgtIds, _chunkEdgeTypes)) {
        const auto srcIt = _nodeIDs.find(src);
        bioassert(srcIt != end(_nodeIDs), "Missing source Node {}", src);

        const auto tgtIt = _nodeIDs.find(tgt);
        bioassert(tgtIt != end(_nodeIDs), "Missing target node {}", tgt);

        const EdgeRecord edgeRecord = dpBuilder.addEdge(typeID, srcIt->second, tgtIt->second);
        _chunkEdgeRecords.push_back(edgeRecord);
    }

    applyProperties();

    _chunkNodeIds = {};
    _chunkNodeIdDefLevels.clear();
    _chunkSrcIdDefLevels.clear();
    _chunkEdgeRecords.clear();
    for (auto& [colIdx, levels] : _propDefLevels) {
        levels.clear();
    }

    return true;
}
