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

// The columns an OPTIONAL CALL was given come back as they were on every row, the rows the
// body yielded nothing for included: only the body's own columns are padded with null
class OptionalSubqueryCarriedColumnTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;

        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

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

// Remy and Adam are the two Founders and the two Persons who know anybody well: the six
// others are padded, and each is a Founder no more than before the CALL
TEST_F(OptionalSubqueryCarriedColumnTest, keepsACarriedLabelTestOnThePaddedRows) {
    expectRows("MATCH (p:Person) WITH p, p:Founder AS founder "
               "OPTIONAL CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS known } "
               "RETURN p.name, founder, known",
               {{"Remy", "true", "Adam"},
                {"Adam", "true", "Remy"},
                {"Maxime", "false", "null"},
                {"Luc", "false", "null"},
                {"Martina", "false", "null"},
                {"Suhas", "false", "null"},
                {"Cyrus", "false", "null"},
                {"Doruk", "false", "null"}});
}

// The carried column reads the same on the rows the body does yield for
TEST_F(OptionalSubqueryCarriedColumnTest, answersAsTheSameQueryWithoutOptional) {
    expectRows("MATCH (p:Person) WITH p, p:Founder AS founder "
               "CALL (p) { MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS known } "
               "RETURN p.name, founder, known",
               {{"Remy", "true", "Adam"},
                {"Adam", "true", "Remy"}});
}
