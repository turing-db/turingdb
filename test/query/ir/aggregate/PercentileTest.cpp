#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "StringRowSink.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

using Rows = std::vector<StringRowSink::Row>;
using ColumnNames = std::vector<std::string>;

}

// percentileCont() and percentileDisc() on the v3 engine. percentileCont interpolates
// between the two values around the percentile's position in the sorted non-null values;
// percentileDisc answers the value at that position. On simpledb the KNOWS_WELL edges
// carry durations 20, 20 and 200, and the INTERESTED_IN edges 10, 15, 20, 20 and 20 beside
// six with no duration at all.
class PercentileTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    void expectNamedRows(std::string_view query, const ColumnNames& names, const Rows& expected) {
        StringRowSink sink;
        runQuery(query, sink);

        EXPECT_EQ(sink.getNames(), names) << "query: " << query;
        EXPECT_EQ(sink.getRows(), expected) << "query: " << query;
    }

    void expectError(std::string_view query, std::string_view reason) {
        StringRowSink sink;

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &sink);

        ASSERT_FALSE(status.isOk()) << "query was accepted: " << query;

        const std::string error = status.getError();
        EXPECT_NE(error.find(reason), std::string::npos) << "query: " << query << "\nerror: " << error;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(PercentileTest, medianOfFourIntegers) {
    expectNamedRows("UNWIND [10, 20, 30, 40] AS v RETURN percentileCont(v, 0.5) AS c, percentileDisc(v, 0.5) AS d",
                    {"c", "d"},
                    {{"25", "20"}});
}

TEST_F(PercentileTest, firstQuartileOfFourIntegers) {
    expectRows("UNWIND [10, 20, 30, 40] AS v RETURN percentileCont(v, 0.25), percentileDisc(v, 0.25)", {{"17.5", "10"}});
}

TEST_F(PercentileTest, valuesAreSortedFirst) {
    expectRows("UNWIND [40, 10, 30, 20] AS v RETURN percentileCont(v, 0.5), percentileDisc(v, 0.5)", {{"25", "20"}});
}

TEST_F(PercentileTest, zeroAndOneAreTheExtremes) {
    expectRows("UNWIND [30, 10, 40, 20] AS v "
               "RETURN percentileCont(v, 0.0), percentileDisc(v, 0.0), percentileCont(v, 1.0), percentileDisc(v, 1.0)",
               {{"10", "10", "40", "40"}});
}

TEST_F(PercentileTest, integerPercentile) {
    expectRows("UNWIND [30, 10, 40, 20] AS v RETURN percentileCont(v, 1), percentileDisc(v, 0)", {{"40", "10"}});
}

TEST_F(PercentileTest, percentileComputedFromConstants) {
    expectRows("UNWIND [10, 20, 30, 40] AS v RETURN percentileCont(v, 1.0 / 4)", {{"17.5"}});
}

TEST_F(PercentileTest, oneValueIsEveryPercentile) {
    expectRows("UNWIND [5] AS v RETURN percentileCont(v, 0.3), percentileDisc(v, 0.3)", {{"5", "5"}});
}

TEST_F(PercentileTest, nullsAreSkipped) {
    expectRows("UNWIND [10, null, 30] AS v RETURN percentileCont(v, 0.5), percentileDisc(v, 0.5)", {{"20", "10"}});
}

TEST_F(PercentileTest, noValueHasNoPercentile) {
    expectRows("MATCH (n) WHERE n.name = 'Nobody' RETURN percentileCont(n.age, 0.5), percentileDisc(n.age, 0.5)",
               {{"null", "null"}});
}

TEST_F(PercentileTest, mixedNumericTypes) {
    expectRows("UNWIND [3, 1, 2.5] AS v RETURN percentileCont(v, 0.5), percentileDisc(v, 0.5)", {{"2.5", "2.5"}});
}

TEST_F(PercentileTest, discreteKeepsTheIntegerType) {
    expectRows("UNWIND [10, 20, 30, 40] AS v RETURN toString(percentileDisc(v, 0.5)), toString(percentileCont(v, 0.5))",
               {{"20", "25.0"}});
}

TEST_F(PercentileTest, percentileOfAPropertySkipsTheEdgesWithoutIt) {
    expectRows("MATCH ()-[e:INTERESTED_IN]->() RETURN percentileCont(e.duration, 0.25), percentileDisc(e.duration, 0.25)",
               {{"15", "15"}});
}

TEST_F(PercentileTest, percentilesPerGroup) {
    const Rows expected = {
        {"INTERESTED_IN", "20", "20"},
        {"KNOWS_WELL", "110", "200"},
    };

    expectRows("MATCH ()-[e]->() RETURN type(e) AS t, percentileCont(e.duration, 0.75), percentileDisc(e.duration, 0.75) "
               "ORDER BY t",
               expected);
}

TEST_F(PercentileTest, percentilesBesideCollectAndCountPerGroup) {
    const Rows expected = {
        {"0", "2, 4, 6", "4", "3", "4"},
        {"1", "1, 3, 5", "3", "3", "3"},
    };

    expectRows("UNWIND [1, 2, 3, 4, 5, 6] AS x "
               "RETURN x % 2 AS parity, collect(x), percentileDisc(x, 0.5), count(x), percentileCont(x, 0.5) "
               "ORDER BY parity",
               expected);
}

TEST_F(PercentileTest, percentilesBesideCollects) {
    expectRows("UNWIND [3, 1, 2, 3] AS v "
               "RETURN collect(v), percentileCont(v, 0.5), percentileDisc(v, 1.0), collect(DISTINCT v)",
               {{"3, 1, 2, 3", "2.5", "3", "3, 1, 2"}});
}

TEST_F(PercentileTest, distinctPercentileCountsEachValueOnce) {
    expectRows("UNWIND [1, 1, 1, 3] AS v RETURN percentileCont(v, 0.5), percentileCont(DISTINCT v, 0.5)", {{"1", "2"}});
}

TEST_F(PercentileTest, distinctPercentilePerGroup) {
    const Rows expected = {
        {"INTERESTED_IN", "15", "15"},
        {"KNOWS_WELL", "110", "20"},
    };

    expectRows("MATCH ()-[e]->() RETURN type(e) AS t, percentileCont(DISTINCT e.duration, 0.5), "
               "percentileDisc(DISTINCT e.duration, 0.5) ORDER BY t",
               expected);
}

TEST_F(PercentileTest, groupsOrderedByAPercentile) {
    const Rows expected = {
        {"0", "4.5"},
        {"2", "3.5"},
        {"1", "2.5"},
    };

    expectRows("UNWIND [1, 2, 3, 4, 5, 6] AS x RETURN x % 3 AS k, percentileCont(x, 0.5) AS m ORDER BY m DESC", expected);
}

TEST_F(PercentileTest, groupsOrderedByAPercentileTheyDoNotReturn) {
    expectRows("UNWIND [1, 2, 3, 4, 5, 6] AS x RETURN x % 3 AS k ORDER BY percentileCont(x, 0.5) DESC",
               {{"0"}, {"2"}, {"1"}});
}

TEST_F(PercentileTest, percentileFeedsAnExpression) {
    expectRows("UNWIND [10, 20, 30, 40] AS v WITH percentileCont(v, 0.5) AS m RETURN m * 2", {{"50"}});
}

TEST_F(PercentileTest, percentileOutsideZeroToOneIsRejected) {
    expectError("UNWIND [10, 20] AS v RETURN percentileCont(v, 1.5)", "between 0.0 and 1.0");
    expectError("UNWIND [10, 20] AS v RETURN percentileDisc(v, -0.5)", "between 0.0 and 1.0");
}

TEST_F(PercentileTest, percentileReadingARowIsRejected) {
    expectError("UNWIND [10, 20] AS v RETURN percentileCont(v, v / 100.0)", "must be constant");
}

TEST_F(PercentileTest, percentileOfAStringIsRejected) {
    expectError("MATCH (n) RETURN percentileDisc(n.name, 0.5)", "percentileDisc");
}
