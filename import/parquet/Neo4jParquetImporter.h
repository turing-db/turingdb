#pragma once

#include <stddef.h>

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
        _reader(_path, _visitor)
    {
    }

    ParquetNeo4jVisitor& visitor() { return _visitor; };

    void import();

private:
    fs::Path _path;
    ParquetNeo4jVisitor _visitor;
    ParquetReader _reader;
};

}
