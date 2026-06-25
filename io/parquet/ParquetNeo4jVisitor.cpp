#include "ParquetNeo4jVisitor.h"

#include <cstdint>
#include <iostream>
#include <string_view>

#include <range/v3/action/sort.hpp>

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "BioAssert.h"

using namespace db;

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
    _chunkLabels = {};
    return true;
}

bool ParquetNeo4jVisitor::onInt64Values(size_t columnIndex, std::span<const int64_t> values) {
    // FIXME: Skips non-node column
    if (columnIndex != _nodeColIdx) {
        return true;
    }

    _chunkNodeIds = values;

    return true;
}

bool ParquetNeo4jVisitor::onByteArrayValues(size_t columnIndex,
                                            std::span<const parquet::ByteArray> values) {
    // FIXME: Skips non-labels column
    if (columnIndex != _lblColIdx) {
        return true;
    }

    _chunkLabels = values;

    return true;
}
