#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// count over a whole list is valid Cypher and the MLIR engine answers it - see
// CypherListLiteralTest - by laying the one list cell out over the driving relation. The
// legacy planner has no such step: it would tally the single row the cell is and answer 1
// where the relation holds more, so the overload is kept out of its reach and the query is
// turned away instead. This pins that: dropping the guard trades a rejection for a wrong
// answer, which no other test would catch.
class AggregateOverListTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);

        _db = &_env->getDB();
    }

protected:
    void expectRejected(std::string_view query) {
        QueryCallbacks callbacks;
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks);
        const QueryStatus status = _db->query(query, state);

        EXPECT_FALSE(status) << "query: " << query;
    }

    void expectAccepted(std::string_view query) {
        QueryCallbacks callbacks;
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks);
        const QueryStatus status = _db->query(query, state);

        EXPECT_TRUE(status) << "query: " << query << ": " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    Graph* _graph {nullptr};
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;
};

TEST_F(AggregateOverListTest, rejectsCountOverAListLiteral) {
    expectRejected("MATCH (n) RETURN count([1, 2])");
}

TEST_F(AggregateOverListTest, rejectsCountOverAListLiteralWithoutAMatch) {
    expectRejected("RETURN count([1, 2])");
}

TEST_F(AggregateOverListTest, rejectsCountOverAnEmptyListLiteral) {
    expectRejected("MATCH (n) RETURN count([])");
}

TEST_F(AggregateOverListTest, stillCountsANodeColumn) {
    // The guard is on the list overload alone: every other count keeps working here.
    expectAccepted("MATCH (n) RETURN count(n)");
}
