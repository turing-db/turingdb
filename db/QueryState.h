#pragma once

#include <string_view>

#include "QueryConfig.h"
#include "QueryCallbacks.h"
#include "versioning/CommitHash.h"
#include "versioning/ChangeID.h"

namespace db {

class LocalMemory;

class QueryState {
public:
    QueryState(std::string_view graphName,
               LocalMemory* mem,
               const QueryConfig* queryConfig,
               const QueryCallbacks* callbacks,
               CommitHash hash = CommitHash::head(),
               ChangeID change = ChangeID::head())
        : _graphName(graphName),
        _mem(mem),
        _queryConfig(queryConfig),
        _callbacks(callbacks),
        _hash(hash),
        _change(change)
    {
    }

    std::string_view getGraphName() const { return _graphName; }
    LocalMemory* getMemory() const { return _mem; }
    const QueryConfig* getQueryConfig() const { return _queryConfig; }
    const QueryCallbacks* getCallbacks() const { return _callbacks; }
    CommitHash getCommitHash() const { return _hash; }
    ChangeID getChangeID() const { return _change; }

private:
    std::string_view _graphName;
    LocalMemory* _mem {nullptr};
    const QueryConfig* _queryConfig {nullptr};
    const QueryCallbacks* _callbacks {nullptr};
    CommitHash _hash {CommitHash::head()};
    ChangeID _change {ChangeID::head()};
};

}
