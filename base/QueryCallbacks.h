#pragma once

#include <functional>

namespace db {

class Block;
class QueryCommand;
class Dataframe;
class QueryStatus;

class QueryCallbacks {
public:
    using ExecTimeMilliseconds = float;

    /**
     * @brief Callback for when the query processing begins (before the transaction is open).
     * */
    using OnBegin = std::function<void()>;

    /**
     * @brief Callback for when an error occurs during the query processing/execution.
     * */
    using OnError = std::function<void(const QueryStatus&)>;

    /**
     * @brief Callback for when the query outputs the header of the dataframe.
     * */
    using OnOutputHeader = std::function<void(const Dataframe*)>;

    /**
     * @brief Callback for when the query outputs data.
     * */
    using OnOutputData = std::function<void(const Dataframe*)>;

    /**
     * @brief Callback for when the query execution is finished .
     * */
    using OnEnd = std::function<void(ExecTimeMilliseconds)>;

    QueryCallbacks();
    ~QueryCallbacks();

    QueryCallbacks(const QueryCallbacks&);
    QueryCallbacks(QueryCallbacks&&);
    QueryCallbacks& operator=(const QueryCallbacks&);
    QueryCallbacks& operator=(QueryCallbacks&&);

    /**
     * @brief Set the callback for when the query processing begins (before the transaction is open).
     *
     * @param onBegin The callback to set.
     * */
    void setOnBegin(const OnBegin& onBegin) {
        _onBegin = onBegin;
    }

    /**
     * @brief Set the callback for when an error occurs during the query processing/execution.
     *
     * @param onError The callback to set.
     * */
    void setOnError(const OnError& onError) {
        _onError = onError;
    }

    /**
     * @brief Set the callback for when the query outputs the header of the dataframe
     *
     * @param onOutputHeader The callback to set.
     * */
    void setOnOutputHeader(const OnOutputHeader& onOutputHeader) {
        _onOutputHeader = onOutputHeader;
    }

    /**
     * @brief Set the callback for when the query outputs data
     *
     * @param onOutputData The callback to set.
     * */
    void setOnOutputData(const OnOutputData& onOutputData) {
        _onOutputData = onOutputData;
    }

    /**
     * @brief Set the callback for when the query execution is finished
     *
     * @param onEnd The callback to set.
     * */
    void setOnEnd(const OnEnd& onEnd) {
        _onEnd = onEnd;
    }

    void onBegin() const;
    void onOutputData(const Dataframe* dataframe) const;
    void onError(const QueryStatus& status) const;
    void onOutputHeader(const Dataframe* dataframe) const;
    void onEnd(ExecTimeMilliseconds time) const;

    static OnBegin defaultOnBegin();
    static OnOutputData defaultOnOutputData();
    static OnError defaultOnError();
    static OnOutputHeader defaultOnOutputHeader();
    static OnEnd defaultOnEnd();

private:
    OnBegin _onBegin;
    OnOutputData _onOutputData;
    OnError _onError;
    OnOutputHeader _onOutputHeader;
    OnEnd _onEnd;
};

}
