#pragma once

#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"

namespace db {
class Column;
}

namespace turing::test {

// Reads every result cell as text, so a test compares rows of strings whatever kinds the
// columns are: an ID or a plain scalar as its number or string, a nullable value as the
// value or "null", a list as its elements joined by ", ".
class StringRowSink : public db::NLOutputSink {
public:
    using Row = std::vector<std::string>;

    StringRowSink();
    ~StringRowSink() override;

    void setColumnNames(std::span<const std::string_view> names) override;
    void appendChunks(std::span<const db::Column* const> chunks, size_t offset, size_t rowCount) override;

    const std::vector<Row>& getRows() const { return _rows; }
    const std::vector<std::string>& getNames() const { return _names; }
    void sortedRows(std::vector<Row>& rows) const;

private:
    std::vector<Row> _rows;
    std::vector<std::string> _names;

    static std::string cellText(const db::Column* chunk, size_t rowIndex);
};

}
