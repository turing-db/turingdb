#pragma once

#include <functional>

#include "QueryStatus.h"

namespace db {

class Block;
class QueryCommand;
class Dataframe;

class QueryCallbacks {
public:
    using OnBegin = std::function<void()>;
    using OnError = std::function<void(const QueryStatus&)>;

    using OnOutputData = std::function<void(const Dataframe*)>;
    using OnOutputHeader = std::function<void(const Dataframe*)>;

    QueryCallbacks() = default;
    ~QueryCallbacks() = default;

    QueryCallbacks(const QueryCallbacks&) = default;
    QueryCallbacks(QueryCallbacks&&) = default;
    QueryCallbacks& operator=(const QueryCallbacks&) = default;
    QueryCallbacks& operator=(QueryCallbacks&&) = default;

    void setOnBegin(OnBegin&& onBegin) {
        _onBegin = std::move(onBegin);
    }

    void setOnOutputData(OnOutputData&& onOutputData) {
        _onOutputData = std::move(onOutputData);
    }

    void setOnError(OnError&& onError) {
        _onError = std::move(onError);
    }

    void setOnOutputHeader(OnOutputHeader&& onOutputHeader) {
        _onOutputHeader = std::move(onOutputHeader);
    }

    void onBegin() {
        _onBegin();
    }

    void onOutputData(const Dataframe* dataframe) {
        _onOutputData(dataframe);
    }

    void onError(const QueryStatus& status) {
        _onError(status);
    }

    void onOutputHeader(const Dataframe* dataframe) {
        _onOutputHeader(dataframe);
    }

    static OnBegin defaultOnBegin() {
        return [] {};
    }

    static OnOutputData defaultOnOutputData() {
        return [](const Dataframe*) {};
    }

    static OnError defaultOnError() {
        return [](const QueryStatus&) {};
    }

    static OnOutputHeader defaultOnOutputHeader() {
        return [](const Dataframe*) {};
    }

private:
    OnBegin _onBegin = defaultOnBegin();
    OnOutputData _onOutputData = defaultOnOutputData();
    OnError _onError = defaultOnError();
    OnOutputHeader _onOutputHeader = defaultOnOutputHeader();
};

}
