#pragma once

namespace db {

class QueryCommand;

class QueryAnalyzer {
public:
    QueryAnalyzer(QueryCommand* cmd);
    ~QueryAnalyzer();

    bool analyze();

private:
    QueryCommand* _cmd {nullptr};
};

}