#include "ForgeConnection.h"

#include "ForgeWorkerThread.h"
#include "BioAssert.h"

using namespace forge;

namespace {
const db::QueryCallbacks::OnOutputData noopCallback = [](const db::Dataframe*) {};

bool extractChangeID(const db::Dataframe* dataframe, db::ChangeID& outChangeID) {
    const auto& columns = dataframe->cols();
    if (columns.empty()) {
        return false;
    }

    auto* changeIDColumn = dynamic_cast<db::ColumnVector<db::ChangeID>*>(columns.front()->getColumn());
    if (!changeIDColumn || changeIDColumn->size() == 0) {
        return false;
    }

    outChangeID = changeIDColumn->getCopy(0);
    return true;
}

uint64_t microsBetween(TimePoint start, TimePoint end) {
    bioassert(end >= start, "microsBetween: end must not precede start");
    return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count());
}

uint64_t engineMicros(const db::QueryStatus& res) {
    const int64_t engineUs = std::chrono::duration_cast<std::chrono::microseconds>(res.getTotalTime()).count();
    return engineUs > 0 ? static_cast<uint64_t>(engineUs) : static_cast<uint64_t>(0);
};

}

ForgeConnection::ForgeConnection(ForgeWorkerThread* thread,
                                 const std::vector<std::string>& queries,
                                 bool isWriteLoad)
        : _client(thread->getConfig()->getAddress(), thread->getConfig()->getPort(), thread->getMemory()),
        _thread(thread),
        _queries(queries),
        _isWriteLoad(isWriteLoad)
{
    _client.setGraphName(thread->getConfig()->getGraphName());
}

void ForgeConnection::beginCycle() {
    auto* client = getClient();
    const TimePoint now = Clock::now();

    if (_isWriteLoad) {
        setCycleStartTime(now);
        client->setChangeID(db::ChangeID::head());
        auto onNewChange = [this](const db::Dataframe* dataframe) {
            db::ChangeID changeID {db::ChangeID::head()};
            if (extractChangeID(dataframe, changeID)) {
                setCapturedChangeID(changeID);
            }
        };
        client->sendQuery("CHANGE NEW", onNewChange);
        setPhase(ForgeConnection::Phase::RunQuery);
    } else {
        setQueryStartTime(now);
        client->sendQuery(_queries[getQueryIndex()], noopCallback);
    }
}



void ForgeConnection::advanceCycle() {
    auto* client = getClient();
    const TimePoint now = Clock::now();
    const db::QueryStatus& res = client->getQueryStatus();

    auto* wallClockHistogram = _thread->getWallClockHistogram();
    auto* engineTimeHistogram = _thread->getEngineHistogram();
    auto* changeCycleHistogram = _thread->getChangeCycleHistogram();
    auto* errors = _thread->getErrorCounter();

    // Warmup gate: only record once the clock has passed the warmup period.
    const bool measuring = now >= _thread->getMeasurementStart();

    if (!_isWriteLoad) {
        if (measuring) {
            if (res.isOk()) {
                wallClockHistogram->record(microsBetween(getQueryStartTime(), now));
                engineTimeHistogram->record(engineMicros(res));
            } else {
                errors->record(res.getStatus());
            }
        }

        advanceQueryIndex(_queries.size());
        beginCycle();
        return;
    }

    // A failed phase aborts the cycle: count it and start a fresh change on the next query.
    if (!res.isOk()) {
        if (measuring) {
            errors->record(res.getStatus());
        }
        advanceQueryIndex(_queries.size());
        beginCycle();
        return;
    }

    switch (getPhase()) {
        case ForgeConnection::Phase::RunQuery: {
            // The change exists and its id was captured from the response; run the create on it.
            client->setChangeID(getCapturedChangeID());
            setQueryStartTime(Clock::now());
            client->sendQuery(_queries[getQueryIndex()], noopCallback);
            setPhase(ForgeConnection::Phase::Submit);
        }
            break;

        case ForgeConnection::Phase::Submit: {
            // the wall-clock histogram only measures the create query itself.
            if (measuring) {
                wallClockHistogram->record(microsBetween(getQueryStartTime(), now));
                engineTimeHistogram->record(engineMicros(res));
            }
            client->sendQuery("CHANGE SUBMIT", noopCallback);
            setPhase(ForgeConnection::Phase::ChangeNew);
        }
            break;

        case ForgeConnection::Phase::ChangeNew: {
            // One whole CHANGE NEW -> create -> CHANGE SUBMIT cycle done.
            // We can start a new one.
            if (measuring) {
                changeCycleHistogram->record(microsBetween(getCycleStartTime(), now));
            }
            advanceQueryIndex(_queries.size());
            beginCycle();
        }
            break;
    }

}
