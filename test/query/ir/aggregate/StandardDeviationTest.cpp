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

// stDev() and stDevP() on the v3 engine: the sample and the population standard deviation
// of the non-null values, 0 where there are too few values to spread. On simpledb the
// KNOWS_WELL edges carry durations 20, 20 and 200, and the INTERESTED_IN edges 20, 20, 20,
// 15 and 10 beside six with no duration at all.
class StandardDeviationTest : public TuringTest {
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

TEST_F(StandardDeviationTest, sampleDeviationOfTwoDoubles) {
    expectNamedRows("UNWIND [1.0, 3.0] AS v RETURN stDev(v) AS s", {"s"}, {{"1.4142135623730951"}});
}

TEST_F(StandardDeviationTest, populationDeviationOfTwoDoubles) {
    expectRows("UNWIND [1.0, 3.0] AS v RETURN stDevP(v)", {{"1"}});
}

TEST_F(StandardDeviationTest, deviationsOfIntegers) {
    expectRows("UNWIND [2, 4, 4, 4, 5, 5, 7, 9] AS v RETURN stDev(v), stDevP(v)", {{"2.138089935299395", "2"}});
}

// A sum of squares minus the squared sum cancels to noise at this magnitude; the spread
// of the values themselves is exactly 1
TEST_F(StandardDeviationTest, deviationOfLargeCloseValuesIsExact) {
    expectRows("UNWIND [1000000001.0, 1000000002.0, 1000000003.0] AS v RETURN stDev(v)", {{"1"}});
}

TEST_F(StandardDeviationTest, oneValueHasNoSpread) {
    expectRows("UNWIND [7.5] AS v RETURN stDev(v), stDevP(v)", {{"0", "0"}});
}

TEST_F(StandardDeviationTest, noValueHasNoSpread) {
    expectRows("MATCH (n) WHERE n.name = 'Nobody' RETURN stDev(n.age), stDevP(n.age)", {{"0", "0"}});
}

TEST_F(StandardDeviationTest, deviationOfTheNullLiteralIsZero) {
    expectRows("MATCH (n) RETURN stDev(null), stDevP(null)", {{"0", "0"}});
}

TEST_F(StandardDeviationTest, nullsAreSkipped) {
    expectRows("UNWIND [1.0, null, 3.0] AS v RETURN stDev(v), count(v)", {{"1.4142135623730951", "2"}});
}

TEST_F(StandardDeviationTest, mixedNumericTypesDeviateAsDoubles) {
    expectRows("UNWIND [1, 2.0, 3] AS v RETURN stDev(v), stDevP(v)", {{"1", "0.816496580927726"}});
}

TEST_F(StandardDeviationTest, deviationOfAPropertySkipsTheEdgesWithoutIt) {
    expectRows("MATCH ()-[e:INTERESTED_IN]->() RETURN stDev(e.duration), stDevP(e.duration)",
               {{"4.47213595499958", "4"}});
}

TEST_F(StandardDeviationTest, deviationsPerGroup) {
    const Rows expected = {
        {"INTERESTED_IN", "4.47213595499958", "4"},
        {"KNOWS_WELL", "103.92304845413264", "84.8528137423857"},
    };

    expectRows("MATCH ()-[e]->() RETURN type(e) AS t, stDev(e.duration), stDevP(e.duration) ORDER BY t", expected);
}

TEST_F(StandardDeviationTest, groupWithOneValueHasNoSpread) {
    const Rows expected = {
        {"Adam", "0", "0"},
        {"Luc", "3.5355339059327378", "2.5"},
        {"Maxime", "0", "0"},
    };

    expectRows("MATCH (a)-[e]->() WHERE a.name IN ['Adam', 'Luc', 'Maxime'] "
               "RETURN a.name AS n, stDev(e.duration), stDevP(e.duration) ORDER BY n",
               expected);
}

TEST_F(StandardDeviationTest, distinctDeviationCountsEachValueOnce) {
    expectRows("UNWIND [1.0, 1.0, 1.0, 3.0] AS v RETURN stDevP(DISTINCT v), stDevP(v)",
               {{"1", "0.8660254037844386"}});
}

TEST_F(StandardDeviationTest, distinctDeviationPerGroup) {
    const Rows expected = {
        {"INTERESTED_IN", "5", "4.08248290463863"},
        {"KNOWS_WELL", "127.27922061357856", "90"},
    };

    expectRows("MATCH ()-[e]->() RETURN type(e) AS t, stDev(DISTINCT e.duration), stDevP(DISTINCT e.duration) ORDER BY t",
               expected);
}

TEST_F(StandardDeviationTest, distinctDeviationOfMixedNumericTypes) {
    expectRows("UNWIND [1, 1, 3.0, null] AS v RETURN stDevP(DISTINCT v)", {{"1"}});
}

TEST_F(StandardDeviationTest, deviationBesideTheOtherAggregates) {
    expectRows("UNWIND [1.0, 3.0] AS v RETURN count(v), avg(v), stDevP(v), collect(v)", {{"2", "2", "1", "1, 3"}});
}

TEST_F(StandardDeviationTest, deviationFeedsAnExpression) {
    expectRows("UNWIND [1.0, 3.0] AS v WITH stDevP(v) AS s RETURN s * 2", {{"2"}});
}

TEST_F(StandardDeviationTest, deviationOfAStringIsRejected) {
    expectError("MATCH (n) RETURN stDev(n.name)", "stDev");
}
