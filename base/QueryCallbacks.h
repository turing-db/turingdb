#pragma once

#include <functional>

namespace db {

class Block;
class QueryCommand;
class Dataframe;
class QueryStatus;

class QueryCallbacks {
public:
    /**
     * @brief Callback for when the query processing begins (before the transaction is open)
     * */
    using OnBegin = std::function<void()>;

    /**
     * @brief Callback for when an error occurs during the query processing/execution
     * */
    using OnError = std::function<void(const QueryStatus&)>;

    /**
     * @brief Callback for when the query outputs the header of the dataframe
     * */
    using OnOutputHeader = std::function<void(const Dataframe*)>;

    /**
     * @brief Callback for when the query outputs data
     * */
    using OnOutputData = std::function<void(const Dataframe*)>;

    QueryCallbacks() = default;
    ~QueryCallbacks() = default;

    QueryCallbacks(const QueryCallbacks&) = default;
    QueryCallbacks(QueryCallbacks&&) = default;
    QueryCallbacks& operator=(const QueryCallbacks&) = default;
    QueryCallbacks& operator=(QueryCallbacks&&) = default;

    /**
     * @brief Set the callback for when the query processing begins (before the transaction is open).
     *
     * @param onBegin The callback to set.
     * */
    void setOnBegin(OnBegin&& onBegin) {
        _onBegin = std::move(onBegin);
    }

    /**
     * @brief Set the callback for when an error occurs during the query processing/execution.
     *
     * @param onError The callback to set.
     * */
    void setOnError(OnError&& onError) {
        _onError = std::move(onError);
    }

    /**
     * @brief Set the callback for when the query outputs the header of the dataframe
     *
     * @param onOutputHeader The callback to set.
     * */
    void setOnOutputHeader(OnOutputHeader&& onOutputHeader) {
        _onOutputHeader = std::move(onOutputHeader);
    }

    /**
     * @brief Set the callback for when the query outputs data
     *
     * @param onOutputData The callback to set.
     * */
    void setOnOutputData(OnOutputData&& onOutputData) {
        _onOutputData = std::move(onOutputData);
    }

    void onBegin();
    void onOutputData(const Dataframe* dataframe);
    void onError(const QueryStatus& status);
    void onOutputHeader(const Dataframe* dataframe);

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
