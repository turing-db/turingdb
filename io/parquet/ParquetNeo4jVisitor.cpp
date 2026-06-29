#include "ParquetNeo4jVisitor.h"

#include <cstdint>
#include <string_view>

#include <range/v3/view/zip.hpp>

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "BioAssert.h"

#include "ID.h"
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

bool ParquetNeo4jVisitor::onFileStart(const parquet::FileMetaData& metadata) {
    const parquet::SchemaDescriptor* sch = metadata.schema();
    const int numCols = metadata.num_columns();

    for (int colIdx = 0; colIdx < numCols; colIdx++) {
        const parquet::ColumnDescriptor* desc = sch->Column(colIdx);
        const std::shared_ptr<parquet::schema::ColumnPath> colPath = desc->path();
        const std::string path = colPath->ToDotString();
        const parquet::Type::type type = desc->physical_type();

        if (path == NEO4J_NODE_COL_PATH) {
            const bool isInt64 = type == NEO4J_NODE_COL_TYPE;
            bioassert(isInt64, "Neo4j node column was not integral.");
            _nodeColIdx = colIdx;
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

        // Otherwise, the column is a property
        // TODO: Get property type and register it here?
        _propCols.push_back(colIdx);
    }

    return true;
}

bool ParquetNeo4jVisitor::onRowGroupStart(size_t, const parquet::RowGroupMetaData&) {
    _chunkNodeIds = {};
    _chunkNodeLabels.clear();
    return true;
}

bool ParquetNeo4jVisitor::onLevels(size_t columnIndex,
                                   std::span<const int16_t> repLevels,
                                   std::span<const int16_t> defLevels) {
    if (columnIndex == _lblColIdx) {
        _chunkLabelRepLevels = repLevels;
        _chunkLabelDefLevels = defLevels;
    }
    return true;
}

bool ParquetNeo4jVisitor::onInt64Values(size_t columnIndex, std::span<const int64_t> values) {
    if (columnIndex == _nodeColIdx) {
        _chunkNodeIds = values;
    } else if (columnIndex == _srcColIdx) {
        _chunkSrcIds = values;
    } else if (columnIndex == _tgtColIdx) {
        _chunkTgtIds = values;
    }

    return true;
}

bool ParquetNeo4jVisitor::onByteArrayValues(size_t columnIndex,
                                            std::span<const parquet::ByteArray> values) {
    if (columnIndex == _lblColIdx) {
        fillLabels(values);
    }

    if (columnIndex == _edgetypeColIdx) {
        fillEdgeTypes(values);
    }

    return true;
}

bool ParquetNeo4jVisitor::onChunkEnd(size_t, size_t, size_t) {
    DataPartBuilder& dpBuilder = _builder->getCurrentBuilder();

    bioassert(_chunkNodeIds.size() == _chunkNodeLabels.size(), "NodeID, Label mismatch");

    for (auto [id, labelIDs] : rv::zip(_chunkNodeIds, _chunkNodeLabels)) {
        LabelSet labelSet;
        for (const LabelID labelID : labelIDs) {
            labelSet.set(labelID);
        }
        _nodeIDs[id] = dpBuilder.addNode(labelSet);
    }
    _chunkNodeLabels.clear();
    _chunkNodeIds = {};

    bioassert(_chunkSrcIds.size() == _chunkTgtIds.size(), "Edge source/target mismatch");
    bioassert(_chunkSrcIds.size() == _chunkEdgeTypes.size(), "Edge, Type mismatch");

    for (auto [src, tgt, typeID] : rv::zip(_chunkSrcIds, _chunkTgtIds, _chunkEdgeTypes)) {
        const auto srcIt = _nodeIDs.find(src);
        bioassert(srcIt != end(_nodeIDs), "Missing source Node {}", src);

        const auto tgtIt = _nodeIDs.find(tgt);
        bioassert(tgtIt != end(_nodeIDs), "Missing target node {}", tgt);

        const NodeID srcID = srcIt->second;
        const NodeID tgtID = tgtIt->second;
        dpBuilder.addEdge(typeID, srcID, tgtID);
    }

    return true;
}
