#pragma once

#include <stddef.h>

#include <memory>
#include <string>
#include <string_view>

#include "QueryConfig.h"
#include "QueryStatus.h"
#include "versioning/ChangeID.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

namespace db {
class NLOutputSink;
class QueryInterpreterV3;
}

namespace turing::test {

// The change plumbing a test of a writing query needs: a SimpleGraph to write into, a
// change per writing query and a submit behind it, so a following read sees what the
// query wrote.
class WriteQueryTest : public TuringTest {
public:
    WriteQueryTest();
    ~WriteQueryTest() override;

protected:
    void initialize() override;

    void openChange(db::ChangeID& changeID);
    void submit(const db::ChangeID& changeID);

    // A reading query at the graph's head, and a writing one in @param changeID
    db::QueryStatus runQuery(std::string_view query, db::NLOutputSink* sink);
    db::QueryStatus runWrite(std::string_view query, const db::ChangeID& changeID);

    // Runs a writing query in its own change and submits it, so a following read sees it
    void applyWrite(std::string_view query);

    // The rows a writing query emits, collected in its own change and then submitted, so
    // a following read sees what it wrote
    void writeRows(std::string_view query, Rows& rows);

    void expectWriteRows(std::string_view query, const Rows& expected);

    // For the entity columns, whose IDs the graph hands out: the count of rows is what
    // the projection is asked about, not the values
    void expectWriteRowCount(std::string_view query, size_t expected);

    // The rows a reading query emits at the graph's head, behind every change a
    // preceding write submitted
    void expectRows(std::string_view query, const Rows& expected);
    void expectRowsInOrder(std::string_view query, const Rows& expected);
    void expectCounts(std::string_view query, const Counts& expected);

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<db::QueryInterpreterV3> _interpreter;
    db::QueryConfig _queryConfig;
};

}
