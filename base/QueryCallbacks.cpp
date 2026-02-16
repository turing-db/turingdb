#include "QueryCallbacks.h"

using namespace db;

QueryCallbacks::QueryCallbacks()
    : _onBegin {defaultOnBegin()},
    _onOutputData {defaultOnOutputData()},
    _onError {defaultOnError()},
    _onOutputHeader {defaultOnOutputHeader()},
    _onEnd {defaultOnEnd()}
{
}

QueryCallbacks::~QueryCallbacks() = default;

QueryCallbacks::QueryCallbacks(const QueryCallbacks&) = default;
QueryCallbacks::QueryCallbacks(QueryCallbacks&&) noexcept = default;
QueryCallbacks& QueryCallbacks::operator=(const QueryCallbacks&) = default;
QueryCallbacks& QueryCallbacks::operator=(QueryCallbacks&&) noexcept = default;

void QueryCallbacks::onBegin() const {
    _onBegin();
}

void QueryCallbacks::onOutputData(const Dataframe* dataframe) const {
    _onOutputData(dataframe);
}

void QueryCallbacks::onError(const QueryStatus& status) const {
    _onError(status);
}

void QueryCallbacks::onOutputHeader(const Dataframe* dataframe) const {
    _onOutputHeader(dataframe);
}

void QueryCallbacks::onEnd(ExecTimeMilliseconds time) const {
    _onEnd(time);
}

QueryCallbacks::OnBegin QueryCallbacks::defaultOnBegin() {
    return [] {};
}

QueryCallbacks::OnOutputData QueryCallbacks::defaultOnOutputData() {
    return [](const Dataframe*) {};
}

QueryCallbacks::OnError QueryCallbacks::defaultOnError() {
    return [](const QueryStatus&) {};
}

QueryCallbacks::OnOutputHeader QueryCallbacks::defaultOnOutputHeader() {
    return [](const Dataframe*) {};
}

QueryCallbacks::OnEnd QueryCallbacks::defaultOnEnd() {
    return [](ExecTimeMilliseconds) {};
}
