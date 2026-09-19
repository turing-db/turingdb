#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "iterators/ChunkConfig.h"
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

// OPTIONAL CALL { ... } over a body returning a boolean. A label test lowers to a mask
// column, which has no null to pad with, so the drain has to read it as a nullable value
// column before it collects it
class CallSubqueryProcedureStrategyTest : public TuringTest {
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
        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        for (const size_t chunkSize : _chunkSizes) {
            _interpreter->setChunkSize(chunkSize);

            RowSink sink;
            const QueryStatus status = runQuery(query, &sink);
            ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

            Rows actual;
            sink.sortedRows(actual);

            std::string actualText;
            describeRows(actual, actualText);

            EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\nat chunk size " << chunkSize
                                              << "\ngot:\n" << actualText;
        }
    }

    const std::vector<size_t> _chunkSizes {1, 3, ChunkConfig::CHUNK_SIZE};

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// 8 Persons against the 9 labels
TEST_F(CallSubqueryProcedureStrategyTest, pairsEveryInputRowWithEveryProcedureRow) {
    expectRows("MATCH (p:Person) CALL (p) { CALL db.labels() YIELD label RETURN label } "
               "RETURN count(label)",
               {{"72"}});
}

// 8 Persons against the 2 nodes the call names
TEST_F(CallSubqueryProcedureStrategyTest, pairsEveryInputRowWithACallTakingArguments) {
    expectRows("MATCH (p:Person) CALL (p) { CALL db.getNodes([0, 1]) YIELD id RETURN id } "
               "RETURN count(id)",
               {{"16"}});
}

// 8 Persons against the 9 labels against the 2 edge types
TEST_F(CallSubqueryProcedureStrategyTest, crossesTwoCallsInOneBody) {
    expectRows("MATCH (p:Person) CALL (p) { CALL db.labels() YIELD label "
               "CALL db.edgeTypes() YIELD edgeType RETURN label, edgeType } "
               "RETURN count(label)",
               {{"144"}});
}

// The LIMIT runs the body per input row, and the call is driven again for each
TEST_F(CallSubqueryProcedureStrategyTest, keepsThePairingWhenABreakerRunsTheBodyPerRow) {
    expectRows("MATCH (p:Person) CALL (p) { CALL db.labels() YIELD label RETURN label ORDER BY label LIMIT 2 } "
               "RETURN count(label)",
               {{"16"}});
}

// 15 INTERESTED_IN edges against the 9 labels, the input arriving from a hop rather than a scan
TEST_F(CallSubqueryProcedureStrategyTest, pairsRowsAHopProduced) {
    expectRows("MATCH (p:Person)-[:INTERESTED_IN]->(i) "
               "CALL (i) { CALL db.labels() YIELD label RETURN label } "
               "RETURN count(label)",
               {{"135"}});
}
