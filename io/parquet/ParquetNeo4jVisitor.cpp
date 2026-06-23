#include "ParquetNeo4jVisitor.h"

#include <cstdint>
#include <string_view>

#include <parquet/metadata.h>
#include <parquet/schema.h>
#include <parquet/types.h>

#include "BioAssert.h"
#include "versioning/CommitBuilder.h"
#include "writers/DataPartBuilder.h"

using namespace db;

bool ParquetNeo4jVisitor::onFileStart(const parquet::FileMetaData& metadata) {
    const parquet::SchemaDescriptor* schema = metadata.schema();
    const int numCols = metadata.num_columns();

    for (int colIdx = 0; colIdx < numCols; colIdx++) {
        const parquet::ColumnDescriptor* desc = schema->Column(colIdx);
        const std::string_view name = desc->name();
        const parquet::Type::type type = desc->physical_type();

        if (name == NEO4J_NODE_COL_NAME) {
            const bool isInt64 = type == NEO4J_NODE_COL_TYPE;
            bioassert(isInt64, "Neo4j node column was not integral.");
            _nodeColIdx = colIdx;
            continue;
        }

        if (name == NEO4J_LBLS_COL_NAME) {
            const bool isLists = type == NEO4J_LBLS_COL_TYPE;
            bioassert(isLists, "Neo4j labels column was not a byte array.");
            _lblColIdx = colIdx;
            continue;
        }

        if (name == NEO4J_SRC_COL_NAME) {
            const bool isInt64 = type == NEO4J_NODE_COL_TYPE;
            bioassert(isInt64, "Neo4j edge source column was not integral.");
            _srcColIdx = colIdx;
            continue;
        }

        if (name == NEO4J_TGT_COL_NAME) {
            const bool isInt64 = type == NEO4J_NODE_COL_TYPE;
            bioassert(isInt64, "Neo4j edge target column was not integral.");
            _tgtColIdx = colIdx;
            continue;
        }

        if (name == NEO4J_ETYPE_COL_NAME) {
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

bool ParquetNeo4jVisitor::onInt64Values(size_t columnIndex, std::span<const int64_t> values) {
    // FIXME: Skips non-node column
    if (columnIndex != _nodeColIdx) {
        return true;
    }

    LabelID l = _builder->metadata().getOrCreateLabel("Person");
    LabelSet ls;
    ls.set(l);
    LabelSetHandle lsh = _builder->metadata().getOrCreateLabelSet(ls);
    for (size_t n = 0; n < values.size(); n++) {
        _builder->getCurrentBuilder().addNode(lsh);
    }
    return true;
}
