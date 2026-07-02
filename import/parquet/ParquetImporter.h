#pragma once

#include <utility>

#include "ParquetImportVisitor.h"

#include "Path.h"

namespace db {

class CommitBuilder;

// Imports a split Parquet export made of two parquet files: one holding the nodes
// (`__id`, `__labels`, node properties) and one holding the edges (`__source`,
// `__target`, `__type`, edge properties). The node file is imported first so that
// every edge endpoint is already mapped to a NodeID by the time the edges are read.
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
