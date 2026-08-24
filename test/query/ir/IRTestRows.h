#pragma once

#include <stddef.h>
#include <stdint.h>

#include <span>
#include <string>
#include <vector>

#include "NLOutputSink.h"

namespace db {
class Column;
class Dataframe;
}

namespace turing::test {

using Row = std::vector<std::string>;
using Rows = std::vector<Row>;
using Counts = std::vector<uint64_t>;

// Renders one output cell as text, whichever column type carries it
void renderCell(const db::Column* column, size_t row, std::string& out);

// Appends every row of a pipeline dataframe, so a v2 result can be compared against a v3 one
void collectPipelineRows(const db::Dataframe* dataframe, Rows& rows);

// Spells the rows out as C++ initialisers, for a failing comparison to print
void describeRows(const Rows& rows, std::string& out);

// Collects every output row as strings, whatever the column types are, so one sink serves
// every projection a query can end on
class RowSink : public db::NLOutputSink {
public:
    void appendChunks(std::span<const db::Column* const> chunks, size_t offset, size_t rowCount) override;

    const Rows& rows() const { return _rows; }

    void sortedRows(Rows& rows) const;

private:
    Rows _rows;
};

// Collects the ui64 column a count emits. The aggregates come behind the grouping keys,
// so the count is the last chunk whichever key is grouped on
class CountSink : public db::NLOutputSink {
public:
    void appendChunks(std::span<const db::Column* const> chunks, size_t offset, size_t rowCount) override;

    const Counts& counts() const { return _counts; }

    void sortedCounts(Counts& counts) const;

private:
    Counts _counts;
};

// Discards output, for the write queries whose rows are not the point of the test
class NullSink : public db::NLOutputSink {
public:
    void appendChunks(std::span<const db::Column* const> chunks, size_t offset, size_t rowCount) override;
};

}
