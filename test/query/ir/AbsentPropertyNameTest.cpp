#include <gtest/gtest.h>

#include <stddef.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// A property name no node or edge in the graph carries reads as null on every row, the
// same as the null literal: nothing in the graph holds a value for it.
class AbsentPropertyNameTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    QueryStatus runQuery(std::string_view query, NLOutputSink* sink) {
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

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectNoRows(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_TRUE(sink.rows().empty()) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectCounts(std::string_view query, const Counts& expected) {
        CountSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Counts actual;
        sink.sortedCounts(actual);

        EXPECT_EQ(actual, expected) << "query: " << query;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(AbsentPropertyNameTest, ReadsAsNullOnEveryRow) {
    expectRows("MATCH (n:Person) RETURN n.name, n.nosuchprop",
               {{"Remy", "null"},
                {"Adam", "null"},
                {"Maxime", "null"},
                {"Luc", "null"},
                {"Martina", "null"},
                {"Suhas", "null"},
                {"Cyrus", "null"},
                {"Doruk", "null"}});
}

TEST_F(AbsentPropertyNameTest, EqualityAgainstItMatchesNothing) {
    expectNoRows("MATCH (n:Person) WHERE n.nosuchprop = 1 RETURN n.name");
}

TEST_F(AbsentPropertyNameTest, InlineConstraintMatchesNothing) {
    expectNoRows("MATCH (n:Person {nosuchprop: 1}) RETURN n.name");
}

TEST_F(AbsentPropertyNameTest, IsNullHoldsOnEveryRow) {
    expectCounts("MATCH (n:Person) WHERE n.nosuchprop IS NULL RETURN count(n)", {8});
}

TEST_F(AbsentPropertyNameTest, IsNotNullHoldsOnNoRow) {
    expectCounts("MATCH (n:Person) WHERE n.nosuchprop IS NOT NULL RETURN count(n)", {0});
}

// count skips nulls, so it counts none of the 8 rows it reads
TEST_F(AbsentPropertyNameTest, CountOfItIsZero) {
    expectCounts("MATCH (n:Person) RETURN count(n.nosuchprop)", {0});
}

TEST_F(AbsentPropertyNameTest, EdgePropertyReadsAsNull) {
    expectRows("MATCH (n:Person)-[e:KNOWS_WELL]->(m) RETURN e.nosuchprop",
               {{"null"}, {"null"}});
}

TEST_F(AbsentPropertyNameTest, EdgeInlineConstraintMatchesNothing) {
    expectNoRows("MATCH (n:Person)-[e:KNOWS_WELL {nosuchprop: 1}]->(m) RETURN m.name");
}

// A read creates nothing: the name is still absent for the statement that follows
TEST_F(AbsentPropertyNameTest, ReadingItLeavesTheSchemaAlone) {
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN n.nosuchprop", {{"null"}});
    expectRows("MATCH (n:Person) WHERE n.name = 'Remy' RETURN n.nosuchprop", {{"null"}});
}
