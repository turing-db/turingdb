#pragma once

#include <utility>

#include "ParquetImportVisitor.h"

#include "Path.h"

namespace db {

class CommitBuilder;

/**
 * @brief Given two Parquet files containing nodes and edges in the expected format,
 * builds the graph in @ref _builder
 *
 * @detail Expected format:
 *     - @ref _nodeFile:
 *       Required columns:
 *           - __id : INT64
 *           - __labels : BYTE_ARRAY [ BYTE_ARRAY] (max nesting: 1)
 *       Any other columns are interpreted as properties
 *     - @ref _edgeFile:
 *       Required columns:
 *           - __source : INT64
 *           - __target : INT64
 *           - __type : BYTE_ARRAY (max nesting: 0)
 *       Any other columns are interpreted as properties
 */
class ParquetImporter {
public:
    ParquetImporter(fs::Path nodeFile, fs::Path edgeFile, CommitBuilder* builder)
        : _nodeFile(std::move(nodeFile)),
        _edgeFile(std::move(edgeFile)),
        _builder(builder)
    {
    }

    void import();

private:
    fs::Path _nodeFile;
    fs::Path _edgeFile;

    CommitBuilder* _builder {nullptr};

    // Source ID -> NodeID, filled while importing nodes and consumed by the edges.
    ParquetImportVisitor::IDMap _nodeIDs;
};

}
