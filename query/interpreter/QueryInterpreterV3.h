#pragma once

#include <string_view>

#include "CompilerContext.h"
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
class ExplainReport;
class GraphView;

class QueryInterpreterV3 {
public:
    explicit QueryInterpreterV3(SystemManager* sysMan);
    ~QueryInterpreterV3();

    void setChunkSize(size_t chunkSize) { _chunkSize = chunkSize; }
    void setCompilerContext(CompilerContext* compilerContext) { _compilerContext = compilerContext; }

    void execute(QueryStatus& status,
                 std::string_view query,
                 std::string_view graphName,
                 CommitHash hash,
                 ChangeID changeID,
                 LocalMemory* mem,
                 NLOutputSink* sink);

private:
    SystemManager* _sysMan {nullptr};
    size_t _chunkSize {ChunkConfig::CHUNK_SIZE};
    CompilerContext _ownedCompilerContext;
    CompilerContext* _compilerContext {&_ownedCompilerContext};

    void executeImpl(QueryStatus& status,
                     std::string_view query,
                     std::string_view graphName,
                     CommitHash hash,
                     ChangeID changeID,
                     LocalMemory* mem,
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
