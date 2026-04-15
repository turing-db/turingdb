#pragma once

#include <string>
#include <vector>

namespace db {

class Dataframe;
class QueryStatus;

}

namespace turing::test {

class QueryResultFormatter {
public:
    static void appendHeader(std::vector<std::string>& columnNames,
                             const db::Dataframe* df);

    static void appendRows(std::vector<std::vector<std::string>>& rows,
                           std::vector<std::string>& values,
                           const db::Dataframe* df);

    static std::string formatResultOutput(const db::QueryStatus& status,
                                          const std::vector<std::string>& columnNames,
                                          const std::vector<std::vector<std::string>>& rows);
};

}
