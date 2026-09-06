#include <gtest/gtest.h>

#include <algorithm>
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

constexpr size_t simpleGraphPersonCount = 8;

}

// toInteger() and toFloat() take a number as well as a string: on the type they already
// return they are the identity, and across the two numeric types they truncate and widen.
class NumericConversionTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void runQuery(std::string_view query, StringRowSink& sink, QueryStatus& status) {
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
    }

    void expectRows(std::string_view query, const std::vector<StringRowSink::Row>& expected) {
        StringRowSink sink;
        QueryStatus status;
        runQuery(query, sink, status);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();

        std::vector<StringRowSink::Row> actual;
        sink.sortedRows(actual);

        std::vector<StringRowSink::Row> sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        EXPECT_EQ(actual, sortedExpected) << "query: " << query;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(NumericConversionTest, toIntegerOfAnIntegerProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toInteger(p.age)", {{"32"}});
}

TEST_F(NumericConversionTest, toIntegerOfAnIntegerPropertyThatIsMissingOnSomeNodes) {
    std::vector<StringRowSink::Row> expected {{"32"}, {"32"}};
    for (size_t person = 2; person < simpleGraphPersonCount; person++) {
        expected.push_back({"null"});
    }

    expectRows("MATCH (p:Person) RETURN toInteger(p.age)", expected);
}

TEST_F(NumericConversionTest, toIntegerTruncatesADouble) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toInteger(p.age / 5.0)", {{"6"}});
}

TEST_F(NumericConversionTest, toIntegerTruncatesANegativeDoubleTowardsZero) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toInteger(0 - p.age / 5.0)", {{"-6"}});
}

TEST_F(NumericConversionTest, toFloatWidensAnIntegerProperty) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toFloat(p.age) / 64", {{"0.5"}});
}

TEST_F(NumericConversionTest, toFloatOfADouble) {
    expectRows("MATCH (p:Person {name: 'Remy'}) RETURN toFloat(p.age / 64.0)", {{"0.5"}});
}

TEST_F(NumericConversionTest, filtersOnToIntegerOfAnEdgeProperty) {
    expectRows("MATCH (:Person {name: 'Remy'})-[e:INTERESTED_IN]->(i:Interest) "
               "WHERE toInteger(e.duration) = 20 RETURN i.name",
               {{"Ghosts"}, {"Eighties"}});
}

TEST_F(NumericConversionTest, ordersByToIntegerOfAnIntegerProperty) {
    expectRows("MATCH (p:Person)-[e:INTERESTED_IN]->(i:Interest) "
               "WHERE e.duration = 15 OR e.duration = 10 "
               "RETURN i.name ORDER BY toInteger(e.duration) ASC",
               {{"Cooking"}, {"Computers"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
