#include <gtest/gtest.h>

#include <stddef.h>
#include <algorithm>
#include <fstream>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using ViewColumn = ColumnVector<types::String::Primitive>;
using StringColumn = ColumnVector<std::string>;
using BoolColumn = ColumnVector<types::Bool::Primitive>;
using ChangeIDColumn = ColumnVector<ChangeID>;
using CountColumn = ColumnVector<types::UInt64::Primitive>;

// Keeps the shape of a result table - the column names and how many rows came
// out - which is all most of these commands report, and hands the columns to a
// per-test reader for the ones whose values matter.
class SystemResultSink : public NLOutputSink {
public:
    using RowReader = void (*)(std::span<const Column* const> chunks, size_t row, SystemResultSink& sink);

    explicit SystemResultSink(RowReader reader = nullptr)
        : _reader(reader)
    {
    }

    void setColumnNames(std::span<const std::string_view> names) override {
        _columnNames.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _columnCount = chunks.size();

        for (size_t row = offset; row < offset + rowCount; row++) {
            _rowCount++;

            if (_reader) {
                _reader(chunks, row, *this);
            }
        }
    }

    const std::vector<std::string>& getColumnNames() const { return _columnNames; }
    const std::vector<std::string>& getStrings() const { return _strings; }
    const std::vector<uint64_t>& getNumbers() const { return _numbers; }
    size_t getColumnCount() const { return _columnCount; }
    size_t getRowCount() const { return _rowCount; }

    void addString(std::string_view value) { _strings.emplace_back(value); }
    void addNumber(uint64_t value) { _numbers.push_back(value); }

private:
    RowReader _reader {nullptr};
    std::vector<std::string> _columnNames;
    std::vector<std::string> _strings;
    std::vector<uint64_t> _numbers;
    size_t _columnCount {0};
    size_t _rowCount {0};
};

void readViewColumn(std::span<const Column* const> chunks, size_t row, SystemResultSink& sink) {
    const ViewColumn* const names = static_cast<const ViewColumn*>(chunks.front());
    sink.addString((*names)[row]);
}

void readStringColumn(std::span<const Column* const> chunks, size_t row, SystemResultSink& sink) {
    const StringColumn* const names = static_cast<const StringColumn*>(chunks.front());
    sink.addString((*names)[row]);
}

void readChangeIDColumn(std::span<const Column* const> chunks, size_t row, SystemResultSink& sink) {
    const ChangeIDColumn* const changes = static_cast<const ChangeIDColumn*>(chunks.front());
    sink.addNumber((*changes)[row].get());
}

void readProcedure(std::span<const Column* const> chunks, size_t row, SystemResultSink& sink) {
    const ViewColumn* const names = static_cast<const ViewColumn*>(chunks[0]);
    const StringColumn* const signatures = static_cast<const StringColumn*>(chunks[1]);

    sink.addString((*names)[row]);
    sink.addString((*signatures)[row]);
}

void readCountColumn(std::span<const Column* const> chunks, size_t row, SystemResultSink& sink) {
    const CountColumn* const counts = static_cast<const CountColumn*>(chunks.front());
    sink.addNumber((*counts)[row]);
}

void readAvailableGraph(std::span<const Column* const> chunks, size_t row, SystemResultSink& sink) {
    const StringColumn* const names = static_cast<const StringColumn*>(chunks[0]);
    const BoolColumn* const loaded = static_cast<const BoolColumn*>(chunks[1]);

    sink.addString((*names)[row]);
    sink.addNumber(static_cast<bool>((*loaded)[row]) ? 1 : 0);
}

// Matches only what the change-lifecycle tests create, so the row count alone says
// whether the write is visible from where the query ran. The label is one simpledb
// already carries: a label the graph has never seen is rejected by the analyzer
// rather than matching nothing, which would report an error where the test wants
// the empty result.
constexpr std::string_view matchLifecycleNode = "MATCH (n:Person {name: \"lifecycle\"}) RETURN n";

bool contains(const std::vector<std::string>& values, std::string_view value) {
    return std::ranges::find(values, value) != values.end();
}

}

// The system-level statements - LOAD GRAPH, CHANGE, COMMIT, SHOW ..., the index
// declarations - run through the MLIR engine, which generates one db op per
// statement rather than a traversal.
class SystemCommandTest : public TuringTest {
public:
    void initialize() override {
        const fs::Path turingDir = fs::Path {_outDir} / "turing";
        _env = _syncedOnDisk ? TuringTestEnv::createSyncedOnDisk(turingDir)
                             : TuringTestEnv::create(turingDir);

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query,
                  QueryStatus& status,
                  SystemResultSink& sink,
                  ChangeID changeID) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &_env->getMem(),
                              &sink);
    }

    void runQuery(std::string_view query, QueryStatus& status, SystemResultSink& sink) {
        runQuery(query, status, sink, ChangeID::head());
    }

    void runQuery(std::string_view query, QueryStatus& status) {
        SystemResultSink sink;
        runQuery(query, status, sink);
    }

    void writeDataFile(std::string_view name, std::string_view contents) {
        const fs::Path path = _env->getConfig().getDataDir() / name;

        std::ofstream file(path.get());
        file << contents;
    }

    const std::string _graphName = "simpledb";
    bool _syncedOnDisk {false};
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// LIST AVAILABLE GRAPHS reads the graphs directory, so a graph is only available
// once it is on disk - which, in full in-memory mode, it never becomes: creating one
// writes nothing and dumping one is a no-op that reports success.
class SystemCommandOnDiskTest : public SystemCommandTest {
public:
    SystemCommandOnDiskTest() {
        _syncedOnDisk = true;
    }
};

TEST_F(SystemCommandTest, listGraphNamesTheLoadedGraph) {
    QueryStatus status;
    SystemResultSink sink(&readViewColumn);
    runQuery("LIST GRAPH", status, sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getColumnCount(), 1u);
    ASSERT_EQ(sink.getColumnNames().size(), 1u);
    EXPECT_EQ(sink.getColumnNames().front(), "graphName");
    EXPECT_TRUE(contains(sink.getStrings(), _graphName));
}

TEST_F(SystemCommandTest, createGraphReportsItsNameAndRegistersIt) {
    QueryStatus createStatus;
    SystemResultSink createSink(&readViewColumn);
    runQuery("CREATE GRAPH other", createStatus, createSink);

    ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();
    ASSERT_EQ(createSink.getRowCount(), 1u);
    EXPECT_EQ(createSink.getStrings().front(), "other");

    QueryStatus listStatus;
    SystemResultSink listSink(&readViewColumn);
    runQuery("LIST GRAPH", listStatus, listSink);

    ASSERT_TRUE(listStatus.isOk()) << listStatus.getError();
    EXPECT_TRUE(contains(listSink.getStrings(), "other"));
    EXPECT_TRUE(contains(listSink.getStrings(), _graphName));
}

TEST_F(SystemCommandOnDiskTest, listAvailableGraphsReportsNameAndLoadState) {
    QueryStatus status;
    SystemResultSink sink(&readAvailableGraph);
    runQuery("LIST AVAILABLE GRAPHS", status, sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getColumnCount(), 3u);
    ASSERT_EQ(sink.getColumnNames().size(), 3u);
    EXPECT_EQ(sink.getColumnNames()[0], "graphName");
    EXPECT_EQ(sink.getColumnNames()[1], "isLoaded");
    EXPECT_EQ(sink.getColumnNames()[2], "isLoading");

    ASSERT_GT(sink.getRowCount(), 0u);

    const std::vector<std::string>& names = sink.getStrings();
    const auto foundIt = std::ranges::find(names, _graphName);
    ASSERT_NE(foundIt, names.end());

    const size_t row = static_cast<size_t>(foundIt - names.begin());
    EXPECT_EQ(sink.getNumbers()[row], 1u) << _graphName << " is loaded, so it must be reported so";
}

TEST_F(SystemCommandTest, changeNewReportsAnIdThatChangeListThenCarries) {
    QueryStatus newStatus;
    SystemResultSink newSink(&readChangeIDColumn);
    runQuery("CHANGE NEW", newStatus, newSink);

    ASSERT_TRUE(newStatus.isOk()) << newStatus.getError();
    ASSERT_EQ(newSink.getRowCount(), 1u);
    ASSERT_EQ(newSink.getColumnNames().size(), 1u);
    EXPECT_EQ(newSink.getColumnNames().front(), "changeID");

    const uint64_t changeID = newSink.getNumbers().front();

    QueryStatus listStatus;
    SystemResultSink listSink(&readChangeIDColumn);
    runQuery("CHANGE LIST", listStatus, listSink);

    ASSERT_TRUE(listStatus.isOk()) << listStatus.getError();

    const std::vector<uint64_t>& openChanges = listSink.getNumbers();
    EXPECT_NE(std::ranges::find(openChanges, changeID), openChanges.end());
}

// COMMIT acts on the change the session selected; without one there is nothing
// to commit, and the command says so rather than silently doing nothing.
TEST_F(SystemCommandTest, commitOutsideAChangeFails) {
    QueryStatus status;
    runQuery("COMMIT", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
}

TEST_F(SystemCommandTest, loadingAnUnknownGraphFails) {
    QueryStatus status;
    runQuery("LOAD GRAPH missing", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
}

TEST_F(SystemCommandTest, showProceduresReportsNamesAndSignatures) {
    QueryStatus status;
    SystemResultSink sink(&readProcedure);
    runQuery("SHOW PROCEDURES", status, sink);

    ASSERT_TRUE(status.isOk()) << status.getError();
    EXPECT_EQ(sink.getColumnCount(), 2u);
    ASSERT_EQ(sink.getColumnNames().size(), 2u);
    EXPECT_EQ(sink.getColumnNames()[0], "name");
    EXPECT_EQ(sink.getColumnNames()[1], "signature");
    EXPECT_GT(sink.getRowCount(), 0u);

    const std::vector<std::string>& rendered = sink.getStrings();
    EXPECT_TRUE(contains(rendered, "db.edgeTypes"));
    EXPECT_TRUE(contains(rendered, "db.edgeTypes() :: (id :: INTEGER, edgeType :: STRING)"));
}

// Merging compacts the data parts and leaves the rows alone, so the node count is
// what says the command did its work rather than dropped some.
TEST_F(SystemCommandTest, mergeDataPartsKeepsEveryNode) {
    QueryStatus beforeStatus;
    SystemResultSink beforeSink;
    runQuery("MATCH (n) RETURN n", beforeStatus, beforeSink);

    ASSERT_TRUE(beforeStatus.isOk()) << beforeStatus.getError();
    ASSERT_GT(beforeSink.getRowCount(), 0u);

    QueryStatus status;
    runQuery("MERGE_DATAPARTS", status);
    ASSERT_TRUE(status.isOk()) << status.getError();

    QueryStatus afterStatus;
    SystemResultSink afterSink;
    runQuery("MATCH (n) RETURN n", afterStatus, afterSink);

    ASSERT_TRUE(afterStatus.isOk()) << afterStatus.getError();
    EXPECT_EQ(afterSink.getRowCount(), beforeSink.getRowCount());
}

TEST_F(SystemCommandTest, vectorIndexIsCreatedListedAndDeleted) {
    QueryStatus createStatus;
    SystemResultSink createSink(&readViewColumn);
    runQuery("CREATE VECTOR INDEX vectors WITH DIMENSION 4 METRIC EUCLID", createStatus, createSink);

    ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();
    ASSERT_EQ(createSink.getRowCount(), 1u);
    EXPECT_EQ(createSink.getStrings().front(), "vectors");

    QueryStatus listStatus;
    SystemResultSink listSink(&readStringColumn);
    runQuery("SHOW VECTOR INDEXES", listStatus, listSink);

    ASSERT_TRUE(listStatus.isOk()) << listStatus.getError();
    EXPECT_EQ(listSink.getColumnCount(), 2u);
    EXPECT_TRUE(contains(listSink.getStrings(), "vectors"));

    QueryStatus deleteStatus;
    SystemResultSink deleteSink(&readViewColumn);
    runQuery("DELETE VECTOR INDEX vectors", deleteStatus, deleteSink);

    ASSERT_TRUE(deleteStatus.isOk()) << deleteStatus.getError();

    QueryStatus emptyStatus;
    SystemResultSink emptySink(&readStringColumn);
    runQuery("SHOW VECTOR INDEXES", emptyStatus, emptySink);

    ASSERT_TRUE(emptyStatus.isOk()) << emptyStatus.getError();
    EXPECT_FALSE(contains(emptySink.getStrings(), "vectors"));
}

// CREATE INDEX and DROP INDEX stage their work on the session's change, so they
// need one open; both run against the change CHANGE NEW just made.
TEST_F(SystemCommandTest, propertyIndexIsCreatedAndDroppedInsideAChange) {
    QueryStatus newStatus;
    SystemResultSink newSink(&readChangeIDColumn);
    runQuery("CHANGE NEW", newStatus, newSink);

    ASSERT_TRUE(newStatus.isOk()) << newStatus.getError();
    ASSERT_EQ(newSink.getRowCount(), 1u);

    const ChangeID changeID {newSink.getNumbers().front()};

    QueryStatus createStatus;
    SystemResultSink createSink;
    runQuery("CREATE INDEX byName FOR (n) ON n.name", createStatus, createSink, changeID);
    ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

    QueryStatus commitStatus;
    SystemResultSink commitSink;
    runQuery("COMMIT", commitStatus, commitSink, changeID);
    ASSERT_TRUE(commitStatus.isOk()) << commitStatus.getError();

    QueryStatus dropStatus;
    SystemResultSink dropSink;
    runQuery("DROP INDEX byName", dropStatus, dropSink, changeID);
    ASSERT_TRUE(dropStatus.isOk()) << dropStatus.getError();

    QueryStatus commitDropStatus;
    SystemResultSink commitDropSink;
    runQuery("COMMIT", commitDropStatus, commitDropSink, changeID);
    ASSERT_TRUE(commitDropStatus.isOk()) << commitDropStatus.getError();

    QueryStatus secondDropStatus;
    SystemResultSink secondDropSink;
    runQuery("DROP INDEX byName", secondDropStatus, secondDropSink, changeID);
    EXPECT_EQ(secondDropStatus.getStatus(), QueryStatus::Status::EXEC_ERROR)
        << "the index is still there, so the first drop did nothing";
}

TEST_F(SystemCommandTest, droppingAnUnknownIndexFails) {
    QueryStatus newStatus;
    SystemResultSink newSink(&readChangeIDColumn);
    runQuery("CHANGE NEW", newStatus, newSink);

    ASSERT_TRUE(newStatus.isOk()) << newStatus.getError();
    ASSERT_EQ(newSink.getRowCount(), 1u);

    const ChangeID changeID {newSink.getNumbers().front()};

    QueryStatus status;
    SystemResultSink sink;
    runQuery("DROP INDEX missing", status, sink, changeID);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
}

// The write path end to end: a change stages a CREATE, COMMIT makes it readable to
// the change itself, and SUBMIT turns it into the graph's history - which is what
// closes the change and puts the node at head.
TEST_F(SystemCommandTest, changeSubmitPublishesTheWriteAndClosesTheChange) {
    QueryStatus newStatus;
    SystemResultSink newSink(&readChangeIDColumn);
    runQuery("CHANGE NEW", newStatus, newSink);

    ASSERT_TRUE(newStatus.isOk()) << newStatus.getError();
    ASSERT_EQ(newSink.getRowCount(), 1u);

    const ChangeID changeID {newSink.getNumbers().front()};

    QueryStatus createStatus;
    SystemResultSink createSink;
    runQuery("CREATE (n:Person {name: \"lifecycle\"})", createStatus, createSink, changeID);
    ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

    QueryStatus bufferedStatus;
    SystemResultSink bufferedSink;
    runQuery(matchLifecycleNode, bufferedStatus, bufferedSink, changeID);
    ASSERT_TRUE(bufferedStatus.isOk()) << bufferedStatus.getError();
    EXPECT_EQ(bufferedSink.getRowCount(), 0u);

    QueryStatus commitStatus;
    SystemResultSink commitSink;
    runQuery("COMMIT", commitStatus, commitSink, changeID);
    ASSERT_TRUE(commitStatus.isOk()) << commitStatus.getError();

    QueryStatus committedStatus;
    SystemResultSink committedSink;
    runQuery(matchLifecycleNode, committedStatus, committedSink, changeID);
    ASSERT_TRUE(committedStatus.isOk()) << committedStatus.getError();
    EXPECT_EQ(committedSink.getRowCount(), 1u);

    QueryStatus unsubmittedHeadStatus;
    SystemResultSink unsubmittedHeadSink;
    runQuery(matchLifecycleNode, unsubmittedHeadStatus, unsubmittedHeadSink);
    ASSERT_TRUE(unsubmittedHeadStatus.isOk()) << unsubmittedHeadStatus.getError();
    EXPECT_EQ(unsubmittedHeadSink.getRowCount(), 0u);

    QueryStatus submitStatus;
    SystemResultSink submitSink(&readChangeIDColumn);
    runQuery("CHANGE SUBMIT", submitStatus, submitSink, changeID);

    ASSERT_TRUE(submitStatus.isOk()) << submitStatus.getError();
    ASSERT_EQ(submitSink.getRowCount(), 1u);
    EXPECT_EQ(submitSink.getColumnNames().front(), "changeID");
    EXPECT_EQ(submitSink.getNumbers().front(), changeID.get());

    QueryStatus listStatus;
    SystemResultSink listSink(&readChangeIDColumn);
    runQuery("CHANGE LIST", listStatus, listSink);

    ASSERT_TRUE(listStatus.isOk()) << listStatus.getError();

    const std::vector<uint64_t>& openChanges = listSink.getNumbers();
    EXPECT_EQ(std::ranges::find(openChanges, changeID.get()), openChanges.end());

    QueryStatus headStatus;
    SystemResultSink headSink;
    runQuery(matchLifecycleNode, headStatus, headSink);
    ASSERT_TRUE(headStatus.isOk()) << headStatus.getError();
    EXPECT_EQ(headSink.getRowCount(), 1u);
}

// The other way a change ends: DELETE drops it, so its commits never reach head.
TEST_F(SystemCommandTest, changeDeleteDiscardsTheWriteAndClosesTheChange) {
    QueryStatus newStatus;
    SystemResultSink newSink(&readChangeIDColumn);
    runQuery("CHANGE NEW", newStatus, newSink);

    ASSERT_TRUE(newStatus.isOk()) << newStatus.getError();
    ASSERT_EQ(newSink.getRowCount(), 1u);

    const ChangeID changeID {newSink.getNumbers().front()};

    QueryStatus createStatus;
    SystemResultSink createSink;
    runQuery("CREATE (n:Person {name: \"lifecycle\"})", createStatus, createSink, changeID);
    ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

    QueryStatus commitStatus;
    SystemResultSink commitSink;
    runQuery("COMMIT", commitStatus, commitSink, changeID);
    ASSERT_TRUE(commitStatus.isOk()) << commitStatus.getError();

    QueryStatus deleteStatus;
    SystemResultSink deleteSink(&readChangeIDColumn);
    runQuery("CHANGE DELETE", deleteStatus, deleteSink, changeID);

    ASSERT_TRUE(deleteStatus.isOk()) << deleteStatus.getError();
    ASSERT_EQ(deleteSink.getRowCount(), 1u);
    EXPECT_EQ(deleteSink.getNumbers().front(), changeID.get());

    QueryStatus listStatus;
    SystemResultSink listSink(&readChangeIDColumn);
    runQuery("CHANGE LIST", listStatus, listSink);

    ASSERT_TRUE(listStatus.isOk()) << listStatus.getError();

    const std::vector<uint64_t>& openChanges = listSink.getNumbers();
    EXPECT_EQ(std::ranges::find(openChanges, changeID.get()), openChanges.end());

    QueryStatus headStatus;
    SystemResultSink headSink;
    runQuery(matchLifecycleNode, headStatus, headSink);
    ASSERT_TRUE(headStatus.isOk()) << headStatus.getError();
    EXPECT_EQ(headSink.getRowCount(), 0u);
}

// Submit and delete act on the change the session selected, the way COMMIT does, so
// both fail the same way when it opened none.
TEST_F(SystemCommandTest, changeSubmitAndDeleteOutsideAChangeFail) {
    QueryStatus submitStatus;
    runQuery("CHANGE SUBMIT", submitStatus);
    EXPECT_EQ(submitStatus.getStatus(), QueryStatus::Status::EXEC_ERROR);

    QueryStatus deleteStatus;
    runQuery("CHANGE DELETE", deleteStatus);
    EXPECT_EQ(deleteStatus.getStatus(), QueryStatus::Status::EXEC_ERROR);
}

// No command may read or write outside the data directory, so a path that climbs
// out of it is refused before the file is ever opened. The index is created first
// because a missing one is reported ahead of the path, which would pass the test
// for the wrong reason.
TEST_F(SystemCommandTest, loadVectorReadsInsideTheDataDirectoryOnly) {
    QueryStatus createStatus;
    SystemResultSink createSink(&readViewColumn);
    runQuery("CREATE VECTOR INDEX vectors WITH DIMENSION 4 METRIC EUCLID", createStatus, createSink);
    ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

    writeDataFile("vectors.csv", "1,1,0,0,0\n2,0,1,0,0\n");

    QueryStatus loadStatus;
    SystemResultSink loadSink(&readCountColumn);
    runQuery("LOAD VECTOR FROM \"vectors.csv\" IN vectors", loadStatus, loadSink);

    ASSERT_TRUE(loadStatus.isOk()) << loadStatus.getError();
    ASSERT_EQ(loadSink.getRowCount(), 1u);
    EXPECT_EQ(loadSink.getColumnNames().front(), "count");
    EXPECT_EQ(loadSink.getNumbers().front(), 2u);

    QueryStatus escapeStatus;
    SystemResultSink escapeSink;
    runQuery("LOAD VECTOR FROM \"../../vectors.csv\" IN vectors", escapeStatus, escapeSink);

    EXPECT_EQ(escapeStatus.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_NE(escapeStatus.getError().find("Invalid file path"), std::string::npos)
        << "the path is what must be refused, not something the file itself failed at: "
        << escapeStatus.getError();
}

// The same guard on the other statement that reads a file the query named. It runs
// inside a change, which LOAD EMBEDDING needs before it looks at the path at all.
TEST_F(SystemCommandTest, loadEmbeddingReadsInsideTheDataDirectoryOnly) {
    QueryStatus newStatus;
    SystemResultSink newSink(&readChangeIDColumn);
    runQuery("CHANGE NEW", newStatus, newSink);

    ASSERT_TRUE(newStatus.isOk()) << newStatus.getError();
    ASSERT_EQ(newSink.getRowCount(), 1u);

    const ChangeID changeID {newSink.getNumbers().front()};

    QueryStatus status;
    SystemResultSink sink;
    runQuery("LOAD EMBEDDING FROM \"../../embeddings.parquet\" AS emb", status, sink, changeID);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_NE(status.getError().find("Invalid file path"), std::string::npos) << status.getError();
}

// A path that stays inside the data directory while still spelling a climb is not
// an escape, so it is resolved rather than refused - what it reaches is then an
// ordinary missing file.
TEST_F(SystemCommandTest, aPathThatClimbsBackInsideTheDataDirectoryIsNotRefused) {
    QueryStatus createStatus;
    SystemResultSink createSink(&readViewColumn);
    runQuery("CREATE VECTOR INDEX vectors WITH DIMENSION 4 METRIC EUCLID", createStatus, createSink);
    ASSERT_TRUE(createStatus.isOk()) << createStatus.getError();

    QueryStatus status;
    SystemResultSink sink;
    runQuery("LOAD VECTOR FROM \"sub/../missing.csv\" IN vectors", status, sink);

    ASSERT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_EQ(status.getError().find("Invalid file path"), std::string::npos)
        << "the path resolves inside the data directory, so the guard must let it through: "
        << status.getError();
}
