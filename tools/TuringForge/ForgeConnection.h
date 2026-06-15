#pragma once

#include "TuringAsyncClient.h"
#include <string>
#include <vector>

namespace forge {

class ForgeWorkerThread;

class ForgeConnection {
public:
    ForgeConnection(ForgeWorkerThread* thread,
                    const std::vector<std::string>& queries,
                    bool isWriteLoad);

    // For a write load, each query runs in its own change: the connection walks the
    // CHANGE NEW -> create -> CHANGE SUBMIT phases per query. Read loads stay in RunQuery.
    enum class Phase {
        ChangeNew,
        RunQuery,
        Submit,
    };

    net::proto::TuringAsyncClient* getClient() { return &_client; }

    size_t getQueryIndex() const { return _queryIndex; }
    void advanceQueryIndex(size_t queryCount) { _queryIndex = (_queryIndex + 1) % queryCount; }

    // Wall-clock start of the main query (the create for a write load); the wall-clock
    // histogram measures only this.
    TimePoint getQueryStartTime() const { return _queryStartTime; }
    void setQueryStartTime(TimePoint startTime) { _queryStartTime = startTime; }

    // Start of the whole CHANGE NEW -> create -> CHANGE SUBMIT cycle (write loads only);
    // the change-cycle histogram measures from here until the submit completes.
    TimePoint getCycleStartTime() const { return _cycleStartTime; }
    void setCycleStartTime(TimePoint startTime) { _cycleStartTime = startTime; }

    Phase getPhase() const { return _phase; }
    void setPhase(Phase phase) { _phase = phase; }

    db::ChangeID getCapturedChangeID() const { return _capturedChangeID; }
    void setCapturedChangeID(db::ChangeID changeID) { _capturedChangeID = changeID; }


    void beginCycle();

    void advanceCycle();

private:
    net::proto::TuringAsyncClient _client;
    ForgeWorkerThread* _thread {nullptr};
    const std::vector<std::string>& _queries;
    bool _isWriteLoad {false};
    TimePoint _queryStartTime;
    TimePoint _cycleStartTime;
    size_t _queryIndex {0};
    Phase _phase {Phase::ChangeNew};
    db::ChangeID _capturedChangeID {db::ChangeID::head()};
};

}
