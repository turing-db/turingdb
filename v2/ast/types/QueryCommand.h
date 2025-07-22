#pragma once

namespace db {

class QueryCommand {
public:
    QueryCommand() = default;
    virtual ~QueryCommand() = 0;

    QueryCommand(const QueryCommand&) = delete;
    QueryCommand(QueryCommand&&) = delete;
    QueryCommand& operator=(const QueryCommand&) = delete;
    QueryCommand& operator=(QueryCommand&&) = delete;
};

inline QueryCommand::~QueryCommand() = default;

}
