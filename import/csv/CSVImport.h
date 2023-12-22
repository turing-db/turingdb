#pragma once

#include "CSVLexer.h"
#include "Writeback.h"

class StringBuffer;

namespace db {
class DB;
class Network;
class Node;
class NodeType;
class EdgeType;
}

class CSVImport {
public:
    struct CSVImportConfig {
        const StringBuffer* buffer = nullptr;
        db::DB* db = nullptr;
        db::Network* outNet = nullptr;
        const char delimiter = ',';
        std::string primaryColumn = "";
    };

    CSVImport(CSVImportConfig&& args);
    ~CSVImport();
    bool run();

private:
    CSVLexer _lexer;
    const StringBuffer* _buffer {nullptr};
    db::DB* _db {nullptr};
    db::Network* _net {nullptr};
    db::Writeback _wb;
    std::string _primaryColumn = "";
};
