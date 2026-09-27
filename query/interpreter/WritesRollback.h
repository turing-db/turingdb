#pragma once

namespace db {

class CommitWriteBuffer;
class CypherAST;
class MetadataBuilder;

// Takes back what a statement staged and interned unless it runs to the end, so a query
// that fails leaves nothing of itself for the commit
class WritesRollback {
public:
    WritesRollback(CommitWriteBuffer* writeBuffer, MetadataBuilder* metadataBuilder);
    ~WritesRollback();

    WritesRollback(const WritesRollback&) = delete;
    WritesRollback& operator=(const WritesRollback&) = delete;

    // A command - COMMIT, CHANGE SUBMIT, a load - can replace the change's buffer as it
    // runs, so only an execution made of queries alone is taken back
    static bool isEnabledFor(const CypherAST& ast);

    void keep();

private:
    CommitWriteBuffer* _writeBuffer {nullptr};
    MetadataBuilder* _metadataBuilder {nullptr};
};

}
