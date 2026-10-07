#include "TuringDB.h"

#include "SystemManager.h"
#include "NLDiscardedOutputSink.h"
#include "QueryInterpreterV3.h"

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
}

QueryStatus TuringDB::query(std::string_view query, const QueryState& state) {
    QueryInterpreterV3 interp(_systemManager.get(), state.getMemory(), state.getCompilerContext());
    interp.setChunkSize(state.getQueryConfig()->getChunkSize());

    NLDiscardedOutputSink discardedSink;
    NLOutputSink* sink = state.getSink();
    if (!sink) {
        sink = &discardedSink;
    }

    QueryStatus status;
    interp.execute(status,
                   query,
                   state.getGraphName(),
                   state.getCommitHash(),
                   state.getChangeID(),
                   sink);

    return status;
}
