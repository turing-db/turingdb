#include "TuringDB.h"

#include <spdlog/spdlog.h>

#include "SystemManager.h"
#include "QueryInterpreterV2.h"
#include "InterpreterContext.h"

using namespace db;

TuringDB::TuringDB(const TuringConfig* config)
    : TuringDB(config, QueryConfig{})
{
}

TuringDB::TuringDB(const TuringConfig* config, const QueryConfig& defaultQueryConfig)
    : _defaultQueryConfig(defaultQueryConfig)
{
    _systemManager = std::make_unique<SystemManager>(config);
}

TuringDB::~TuringDB() {
}

void TuringDB::init() {
    _systemManager->init();

    // Disable value hash join if requested in environment
    const char* vhjEnv = getenv("TURING_VALUE_HASH_JOIN");
    if (vhjEnv) {
        if(strcmp(vhjEnv, "0") == 0) {
            _defaultQueryConfig.getPlanGenConfig().setUseValueHashJoin(false);
        }
    }
}

QueryStatus TuringDB::query(std::string_view query, const QueryState& state) {
    QueryInterpreterV2 interp(_systemManager.get());

    QueryStatus status;
    InterpreterContext ctxt(state.getMemory(),
                            state.getCallbacks(),
                            _systemManager.get(),
                            state.getCommitHash(),
                            state.getChangeID());
    ctxt.setQueryConfig(state.getQueryConfig());
    interp.execute(ctxt, status, query, state.getGraphName());

    return status;
}
