#pragma once

#include <stddef.h>
#include <unordered_map>

#include "ID.h"
#include "ParquetNeo4jVisitor.h"

#include "Path.h"

namespace db {

class ParquetReader;
class CommitBuilder;

class Neo4jParquetImporter {
public:

    explicit Neo4jParquetImporter(fs::Path path, CommitBuilder* builder)
        : _path(std::move(path)),
        _visitor(builder),
        _reader(_path, _visitor),
        _builder(builder)
    {
    }

    ParquetNeo4jVisitor& visitor() { return _visitor; };

    void import();

private:
    // Maps Neo4j IDs to TuringDB IDs
    using IDMap = std::unordered_map<int64_t, NodeID>;

    fs::Path _path;
    ParquetNeo4jVisitor _visitor;
    ParquetReader _reader;

    CommitBuilder* _builder {nullptr};

    void addNodes();
};

}
