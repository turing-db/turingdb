#pragma once

#include "Time.h"

namespace db {

class QueryStatus {
public:
    enum class Status {
        OK,
        PARSE_ERROR,
        ANALYZE_ERROR,
        PLAN_ERROR,
        EXEC_ERROR
    };

    QueryStatus() = default;
    explicit QueryStatus(Status status)
        : _status(status)
    {
    }

    ~QueryStatus() = default;

    Status getStatus() const { return _status; }

    void setTotalTime(Milliseconds totalTime) { _totalTime = totalTime; }
    Milliseconds getTotalTime() const { return _totalTime; }

private:
    Status _status {Status::OK};
    Milliseconds _totalTime {0};
};

}
