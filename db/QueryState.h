#pragma once

#include <string_view>

#include "QueryConfig.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class LocalMemory;
class NLOutputSink;
class CompilerContext;

class QueryState {
public:
    QueryState(std::string_view graphName,
               LocalMemory* mem,
               CompilerContext* compilerContext,
               const QueryConfig* queryConfig,
               NLOutputSink* sink,
               CommitHash hash = CommitHash::head(),
               ChangeID change = ChangeID::head())
        : _graphName(graphName),
        _mem(mem),
        _compilerContext(compilerContext),
        _queryConfig(queryConfig),
        _sink(sink),
        _hash(hash),
        _change(change)
    {
    }

    std::string_view getGraphName() const { return _graphName; }
    LocalMemory* getMemory() const { return _mem; }
    CompilerContext* getCompilerContext() const { return _compilerContext; }
    const QueryConfig* getQueryConfig() const { return _queryConfig; }
    NLOutputSink* getSink() const { return _sink; }
    CommitHash getCommitHash() const { return _hash; }
    ChangeID getChangeID() const { return _change; }

private:
    std::string_view _graphName;
    LocalMemory* _mem {nullptr};
    CompilerContext* _compilerContext {nullptr};
    const QueryConfig* _queryConfig {nullptr};
    NLOutputSink* _sink {nullptr};
    CommitHash _hash {CommitHash::head()};
    ChangeID _change {ChangeID::head()};
};

}
