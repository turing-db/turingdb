#include "ParquetNeo4jVisitor.h"

#include <parquet/types.h>
#include <string_view>

#include <parquet/metadata.h>
#include <parquet/schema.h>

#include "BioAssert.h"

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
            _loader->_nodeColIdx = colIdx;
            continue;
        }

        if (name == NEO4J_LBLS_COL_NAME) {
            const bool isLists = type == NEO4J_LBLS_COL_TYPE;
            bioassert(isLists, "Neo4j labels column was not a byte array.");
            _loader->_lblColIdx = colIdx;
            continue;
        }

        if (name == NEO4J_SRC_COL_NAME) {
            const bool isInt64 = type == NEO4J_NODE_COL_TYPE;
            bioassert(isInt64, "Neo4j edge source column was not integral.");
            _loader->_srcColIdx = colIdx;
            continue;
        }

        if (name == NEO4J_TGT_COL_NAME) {
            const bool isInt64 = type == NEO4J_NODE_COL_TYPE;
            bioassert(isInt64, "Neo4j edge target column was not integral.");
            _loader->_tgtColIdx = colIdx;
            continue;
        }

        if (name == NEO4J_ETYPE_COL_NAME) {
            const bool isString = type == NEO4J_ETYPE_COL_TYPE;
            bioassert(isString, "Neo4j edge type column was not string.");
            _loader->_edgetypeColIdx = colIdx;
            continue;
        }

        // Otherwise, the column is a property
        // TODO: Get property type and register it here?
        _loader->_propCols.push_back(colIdx);
    }
    return true;
}
