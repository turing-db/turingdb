#include <gtest/gtest.h>

#include <memory>
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
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// AFL inputs that threw 'Unsupported binary operation of columns of kinds ...' from a simple
// CASE comparing its operand to a WHEN value of another type: an absent property against an
// integer, and an integer against a boolean.
class FuzzCaseComparisonTest : public TuringTest {
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

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, expected) << "query: " << query << "\nactual:\n" << actualText;
    }

    void expectEveryPerson(std::string_view query, std::string_view value) {
        Rows expected;
        for (const std::string& name : _personNames) {
            expected.push_back({name, std::string(value)});
        }

        expectRows(query, expected);
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
    std::vector<std::string> _personNames {"Adam", "Cyrus", "Doruk", "Luc", "Martina", "Maxime", "Remy", "Suhas"};
};

TEST_F(FuzzCaseComparisonTest, Return000011) {
    expectEveryPerson("MATCH (n:Person) RETURN n.name, CASE n.agu WHEN IS NULL THEN 'Unknown' WHEN = 0, = 1, = 2 THEN 'Baby' WHEN <= 13 THEN 'Child' WHEN < 20 THEN 'Teenager' WHEN < 30 THEN 'Young Adult' WHEN > 1000 THEN 'Immortal' ELSE 'Adult' END",
                      "Unknown");
}

TEST_F(FuzzCaseComparisonTest, Return000013) {
    expectRows("MATCH (n:Person) RETURN n.name, CASE n.age WHEN IS NULL THEN 'Unknown' WHEN = 0 = 2 THEN 'Baby' WHEN <= 13 THEN 'Child' WHEN < 20 THEN 'Teenager' WHEN < 30 THEN 'Young Adult' WHEN > 1000 THEN 'Immortal' ELSE 'Adult' END",
               {{"Adam", "Adult"},
                {"Cyrus", "Unknown"},
                {"Doruk", "Unknown"},
                {"Luc", "Unknown"},
                {"Martina", "Unknown"},
                {"Maxime", "Unknown"},
                {"Remy", "Adult"},
                {"Suhas", "Unknown"}});
}

TEST_F(FuzzCaseComparisonTest, Return000005) {
    expectRows("MATCH (n:Person) RETURN n.name, CASE n.age WHEN n.age * 4> 40 THEN 'over forty' WHEN >= 32 THEN 'thirty-something' WHEN IS NOT NULL THEN '\xff\x7funger' ELSE 'unknown' END",
               {{"Adam", "thirty-something"},
                {"Cyrus", "unknown"},
                {"Doruk", "unknown"},
                {"Luc", "unknown"},
                {"Martina", "unknown"},
                {"Maxime", "unknown"},
                {"Remy", "thirty-something"},
                {"Suhas", "unknown"}});
}

TEST_F(FuzzCaseComparisonTest, ReturnAbsentPropertyIsNull) {
    expectEveryPerson("MATCH (n:Person) RETURN n.name, CASE n.agu WHEN IS NULL THEN 'unknown' ELSE 'known' END", "unknown");
}

TEST_F(FuzzCaseComparisonTest, ReturnAbsentPropertyOrderedAgainstInteger) {
    expectEveryPerson("MATCH (n:Person) RETURN n.name, CASE n.agu WHEN > 13 THEN 'older' ELSE 'other' END", "other");
}

TEST_F(FuzzCaseComparisonTest, ReturnIntegerAgainstBooleanConstant) {
    expectEveryPerson("MATCH (n:Person) RETURN n.name, CASE n.age WHEN false THEN 'false' ELSE 'other' END", "other");
}

TEST_F(FuzzCaseComparisonTest, ReturnIntegerAgainstBooleanColumn) {
    expectEveryPerson("MATCH (n:Person) RETURN n.name, CASE n.age WHEN n.age > 40 THEN 'over forty' ELSE 'other' END", "other");
}

TEST_F(FuzzCaseComparisonTest, ReturnStringAgainstBooleanConstant) {
    expectEveryPerson("MATCH (n:Person) RETURN n.name, CASE n.name WHEN true THEN 'true' ELSE 'other' END", "other");
}
