#pragma once

#include <string>

#include "EnumToString.h"
#include "TuringTime.h"

namespace db {

class QueryStatus {
public:
    enum class Status {
        OK,
        GRAPH_NOT_FOUND,
        PARSE_ERROR,
        ANALYZE_ERROR,
        PLAN_ERROR,
        EXEC_ERROR,
        COMMIT_NOT_FOUND,
        CHANGE_NOT_FOUND,
        _SIZE
    };

    QueryStatus() = default;
    QueryStatus(Status status, const std::string& errorMsg = "")
        : _status(status)
        , _errorMsg(errorMsg)
    {
    }

    ~QueryStatus() = default;

    Status getStatus() const { return _status; }

    bool isOk() const { return _status == Status::OK; }

    operator bool() const { return isOk(); }

    bool hasErrorMessage() const { return !_errorMsg.empty(); }
    const std::string& getError() const { return _errorMsg; }
    void setError(const std::string& error) { _errorMsg = error; }

    void setTotalTime(Milliseconds totalTime) { _totalTime = totalTime; }
    Milliseconds getTotalTime() const { return _totalTime; }

private:
    Status _status {Status::OK};
    std::string _errorMsg;
    Milliseconds _totalTime {0};
};

using QueryStatusDescription = EnumToString<QueryStatus::Status>::Create<
    EnumStringPair<QueryStatus::Status::OK, "OK">,
    EnumStringPair<QueryStatus::Status::GRAPH_NOT_FOUND, "GRAPH_NOT_FOUND">,
    EnumStringPair<QueryStatus::Status::PARSE_ERROR, "PARSE_ERROR">,
    EnumStringPair<QueryStatus::Status::ANALYZE_ERROR, "ANALYZE_ERROR">,
    EnumStringPair<QueryStatus::Status::PLAN_ERROR, "PLAN_ERROR">,
    EnumStringPair<QueryStatus::Status::EXEC_ERROR, "EXEC_ERROR">,
    EnumStringPair<QueryStatus::Status::COMMIT_NOT_FOUND, "COMMIT_NOT_FOUND">,
    EnumStringPair<QueryStatus::Status::CHANGE_NOT_FOUND, "CHANGE_NOT_FOUND">
>;

}
