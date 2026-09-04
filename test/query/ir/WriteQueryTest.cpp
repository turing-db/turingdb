#include "WriteQueryTest.h"

#include <algorithm>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "dataframe/Dataframe.h"
#include "versioning/Change.h"
#include "versioning/CommitHash.h"

using namespace db;
using namespace turing::test;

WriteQueryTest::WriteQueryTest() {
}

WriteQueryTest::~WriteQueryTest() {
}

void WriteQueryTest::initialize() {
    _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
    _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

    SystemAccessor system = _env->getSystemManager().accessUnique();
    Graph* graph = system.createGraph(_graphName);
    SimpleGraph::createSimpleGraph(graph);
}

void WriteQueryTest::openChange(ChangeID& changeID) {
    SystemAccessor system = _env->getSystemManager().accessUnique();
    const auto res = system.newChange(_graphName);
    ASSERT_TRUE(res);

    changeID = res.value()->id();
}

void WriteQueryTest::submit(const ChangeID& changeID) {
    QueryCallbacks callbacks;
    callbacks.setOnOutputData([](const Dataframe*) {});

    const QueryState submitState(_graphName,
                                 &_env->getMem(),
                                 &_queryConfig,
                                 &callbacks,
                                 CommitHash::head(),
                                 changeID);
    const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
    ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
}

QueryStatus WriteQueryTest::runQuery(std::string_view query, NLOutputSink* sink) {
    QueryStatus status;
    _interpreter->execute(status,
                          query,
                          _graphName,
                          CommitHash::head(),
                          ChangeID::head(),
                          &_env->getMem(),
                          sink);

    return status;
}

QueryStatus WriteQueryTest::runWrite(std::string_view query, const ChangeID& changeID) {
    NullSink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          query,
                          _graphName,
                          CommitHash::head(),
                          changeID,
                          &_env->getMem(),
                          &sink);

    return status;
}

void WriteQueryTest::applyWrite(std::string_view query) {
    ChangeID changeID;
    openChange(changeID);

    const QueryStatus status = runWrite(query, changeID);
    ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

    submit(changeID);
}

void WriteQueryTest::writeRows(std::string_view query, Rows& rows) {
    ChangeID changeID;
    openChange(changeID);

    RowSink sink;
    QueryStatus status;
    _interpreter->execute(status,
                          query,
                          _graphName,
                          CommitHash::head(),
                          changeID,
                          &_env->getMem(),
                          &sink);
    ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

    sink.sortedRows(rows);

    submit(changeID);
}

void WriteQueryTest::expectWriteRows(std::string_view query, const Rows& expected) {
    Rows actual;
    writeRows(query, actual);

    Rows sortedExpected = expected;
    std::sort(sortedExpected.begin(), sortedExpected.end());

    std::string actualText;
    describeRows(actual, actualText);

    EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
}

void WriteQueryTest::expectWriteRowCount(std::string_view query, size_t expected) {
    Rows actual;
    writeRows(query, actual);

    EXPECT_EQ(actual.size(), expected) << "query: " << query;
}

void WriteQueryTest::expectRows(std::string_view query, const Rows& expected) {
    RowSink sink;
    const QueryStatus status = runQuery(query, &sink);
    ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

    Rows actual;
    sink.sortedRows(actual);

    Rows sortedExpected = expected;
    std::sort(sortedExpected.begin(), sortedExpected.end());

    std::string actualText;
    describeRows(actual, actualText);

    EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
}

void WriteQueryTest::expectRowsInOrder(std::string_view query, const Rows& expected) {
    RowSink sink;
    const QueryStatus status = runQuery(query, &sink);
    ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

    std::string actualText;
    describeRows(sink.rows(), actualText);

    EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
}

void WriteQueryTest::expectCounts(std::string_view query, const Counts& expected) {
    CountSink sink;
    const QueryStatus status = runQuery(query, &sink);
    ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

    Counts actual;
    sink.sortedCounts(actual);

    EXPECT_EQ(actual, expected) << "query: " << query;
}
