#include <gtest/gtest.h>

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

namespace {

// The seven simpledb people other than Remy, who is node 0
const Rows peopleOtherThanRemy = {
    {"Adam"}, {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"},
};

// The six for whom neither `n = 0` nor `n = 1` holds, so the two agree
const Rows peopleNeitherIDMatches = {
    {"Maxime"}, {"Luc"}, {"Martina"}, {"Suhas"}, {"Cyrus"}, {"Doruk"},
};

}

// A negation produces a mask rather than a plain boolean column, and a mask is a boolean
// column like any other: it can be compared, not only filtered on or combined with AND.
class MaskComparisonTest : public TuringTest {
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

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(MaskComparisonTest, comparesAMaskAgainstALiteral) {
    expectRows("MATCH (n:Person) WHERE (n = 0) = true RETURN n.name", {{"Remy"}});
}

// The same comparison written the other way round: which side the query puts the mask on
// is the query's choice.
TEST_F(MaskComparisonTest, comparesALiteralAgainstAMask) {
    expectRows("MATCH (n:Person) WHERE true = (n = 0) RETURN n.name", {{"Remy"}});
}

TEST_F(MaskComparisonTest, comparesANegatedMaskAgainstALiteral) {
    expectRows("MATCH (n:Person) WHERE (NOT (n = 0)) = true RETURN n.name", peopleOtherThanRemy);
}

TEST_F(MaskComparisonTest, comparesAMaskAgainstAMask) {
    expectRows("MATCH (n:Person) WHERE (n = 0) = (n = 1) RETURN n.name", peopleNeitherIDMatches);
}

TEST_F(MaskComparisonTest, comparesAMaskAgainstAMaskForInequality) {
    expectRows("MATCH (n:Person) WHERE (n = 0) <> (n = 1) RETURN n.name", {{"Remy"}, {"Adam"}});
}

// A property is read as a nullable column, so this pairs the mask with the third boolean
// shape a column comes in.
TEST_F(MaskComparisonTest, comparesAMaskAgainstABooleanProperty) {
    expectRows("MATCH (n:Person) WHERE (NOT (n = 0)) = n.hasPhD RETURN n.name",
               {{"Adam"}, {"Luc"}, {"Martina"}});
}

TEST_F(MaskComparisonTest, comparesABooleanPropertyAgainstAMask) {
    expectRows("MATCH (n:Person) WHERE n.hasPhD = (NOT (n = 0)) RETURN n.name",
               {{"Adam"}, {"Luc"}, {"Martina"}});
}

// What a mask could already do, which comparing one must not disturb
TEST_F(MaskComparisonTest, keepsFilteringAndCombiningOnAMask) {
    expectRows("MATCH (n:Person) WHERE NOT (n = 0) AND n.hasPhD RETURN n.name",
               {{"Adam"}, {"Luc"}, {"Martina"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
