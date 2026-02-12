#include "QueryCallbacks.h"

using namespace db;

void QueryCallbacks::onBegin() {
    _onBegin();
}

void QueryCallbacks::onOutputData(const Dataframe* dataframe) {
    _onOutputData(dataframe);
}

void QueryCallbacks::onError(const QueryStatus& status) {
    _onError(status);
}

void QueryCallbacks::onOutputHeader(const Dataframe* dataframe) {
    _onOutputHeader(dataframe);
}

void QueryCallbacks::onEnd(ExecTimeMilliseconds time) {
    _onEnd(time);
}
