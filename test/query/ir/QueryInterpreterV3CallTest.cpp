#include <gtest/gtest.h>

#include <algorithm>
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
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Collects the single borrowed-string column db.labels emits when only its label return
// value is yielded.
class LabelSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        ASSERT_EQ(chunks.size(), 1u);

        const auto* labels = dynamic_cast<const ColumnVector<types::String::Primitive>*>(chunks[0]);
        ASSERT_NE(labels, nullptr);

        const auto& raw = labels->getRaw();
        for (size_t rowIndex = offset; rowIndex < offset + rowCount; rowIndex++) {
            _rows.emplace_back(raw[rowIndex]);
        }
    }

    void sortedRows(std::vector<std::string>& rows) const {
        rows = _rows;
        std::sort(rows.begin(), rows.end());
    }

private:
    std::vector<std::string> _rows;
};

// Records the names a program gives its result columns, and counts the rows so a named
// result is told apart from no result at all.
class ColumnNameSink : public NLOutputSink {
public:
    void setColumnNames(std::span<const std::string_view> names) override {
        _names.assign(names.begin(), names.end());
    }

    void appendChunks(std::span<const Column* const> chunks, size_t offset, size_t rowCount) override {
        _rowCount += rowCount;
    }

    const std::vector<std::string>& getNames() const { return _names; }
    size_t getRowCount() const { return _rowCount; }

private:
    std::vector<std::string> _names;
    size_t _rowCount {0};
};

}

class QueryInterpreterV3CallTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, QueryStatus& status, NLOutputSink& sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// A CALL resolves its procedure through the registry the interpreter is handed, so the
// registry the system already owns has to reach the engine from this entry point: it is
// the only one a served query goes through.
TEST_F(QueryInterpreterV3CallTest, callProcedureYieldsRows) {
    LabelSink sink;
    QueryStatus status;
    runQuery("CALL db.labels() YIELD label RETURN label", status, sink);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::OK);
    EXPECT_EQ(status.getError(), "");

    std::vector<std::string> rows;
    sink.sortedRows(rows);
    const std::vector<std::string> expected {"Bioinformatics",
                                             "Exotic",
                                             "Founder",
                                             "Interest",
                                             "Person",
                                             "Sales",
                                             "SleepDisturber",
                                             "SoftwareEngineering",
                                             "Supernatural"};
    EXPECT_EQ(rows, expected);
}

// A CALL is a query of its own, and its result is read by column name like any other:
// the yields name the columns even where no RETURN restates them.
TEST_F(QueryInterpreterV3CallTest, standaloneCallNamesItsColumns) {
    ColumnNameSink sink;
    QueryStatus status;
    runQuery("CALL db.labels() YIELD id, label", status, sink);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::OK);
    EXPECT_EQ(status.getError(), "");

    const std::vector<std::string> expected {"id", "label"};
    EXPECT_EQ(sink.getNames(), expected);
    EXPECT_GT(sink.getRowCount(), 0u);
}

TEST_F(QueryInterpreterV3CallTest, standaloneCallWithoutAYieldNamesItsReturnValues) {
    ColumnNameSink sink;
    QueryStatus status;
    runQuery("CALL db.labels()", status, sink);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::OK);
    EXPECT_EQ(status.getError(), "");

    const std::vector<std::string> expected {"id", "label"};
    EXPECT_EQ(sink.getNames(), expected);
    EXPECT_GT(sink.getRowCount(), 0u);
}
