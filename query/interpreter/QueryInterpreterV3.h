#pragma once

#include <string_view>

#include "NLOutputSink.h"
#include "QueryStatus.h"
#include "iterators/ChunkConfig.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace mlir {
class MLIRContext;
}

namespace db {

class SystemManager;
class LocalMemory;
class CompilerContext;
class ExplainReport;
class GraphView;

class QueryInterpreterV3 {
public:
    QueryInterpreterV3(SystemManager* sysMan, LocalMemory* mem, CompilerContext* compilerContext);
    ~QueryInterpreterV3();

    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }

    void execute(QueryStatus& status,
                 std::string_view query,
                 std::string_view graphName,
                 CommitHash hash,
                 ChangeID changeID,
                 NLOutputSink* sink);

private:
    SystemManager* _sysMan {nullptr};
    LocalMemory* _mem {nullptr};
    CompilerContext* _compilerContext {nullptr};
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};

    void executeImpl(QueryStatus& status,
                     std::string_view query,
                     std::string_view graphName,
                     CommitHash hash,
                     ChangeID changeID,
                     NLOutputSink* sink);

    // Emits the dumps an explained query collected, by compiling and running the
    // small program that reports them - so an EXPLAIN returns its rows through the
    // same path as any other statement
    void reportExplain(const ExplainReport& report,
                       mlir::MLIRContext* context,
                       const GraphView* view,
                       LocalMemory* memory,
                       NLOutputSink* sink);
};

}
