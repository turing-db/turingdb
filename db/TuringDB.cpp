#include "TuringDB.h"

#include <span>

#include "SystemManager.h"
#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"

using namespace db;

namespace {

class DiscardedOutputSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {}
};

}

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
    QueryInterpreterV3 interp(_systemManager.get());
    interp.setChunkSize(state.getQueryConfig()->getChunkSize());

    DiscardedOutputSink discardedSink;
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
                   state.getMemory(),
                   sink);

    return status;
}
