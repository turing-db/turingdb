#pragma once

#include <span>
#include <string>
#include <vector>

namespace db {

class Column;
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

    static void appendChunkRows(std::vector<std::vector<std::string>>& rows,
                                std::vector<std::string>& values,
                                std::span<const db::Column* const> chunks,
                                size_t offset,
                                size_t rowCount);

    static std::string formatResultOutput(const db::QueryStatus& status,
                                          const std::vector<std::string>& columnNames,
                                          const std::vector<std::vector<std::string>>& rows);
};

}
