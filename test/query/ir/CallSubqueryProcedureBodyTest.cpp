#include <gtest/gtest.h>

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

// A procedure call writes nothing, so a body ending on one is a reading body and its RETURN
// is mandatory, exactly as for a body ending on MATCH
class CallSubqueryProcedureBodyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);
    }

    void expectRejected(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
        ASSERT_FALSE(status.isOk()) << "query: " << query;

        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    void expectAccepted(std::string_view query) {
        NullSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        EXPECT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// Without a YIELD the call names no columns, and it is only a standalone call - a whole
// query that is one CALL - that may leave them unnamed. A body is never that
TEST_F(CallSubqueryProcedureBodyTest, namesTheMissingYieldOfABodyEndingOnAProcedureCall) {
    expectRejected("CALL { CALL db.labels() } RETURN 1", "requires to name the return items");
    expectRejected("MATCH (p:Person) CALL (p) { CALL db.labels() } RETURN count(p)",
                   "requires to name the return items");
    expectRejected("MATCH (p:Person) CALL (p) { CALL db.labels() }", "requires to name the return items");
}

TEST_F(CallSubqueryProcedureBodyTest, namesTheMissingReturnOfAYieldingProcedureCall) {
    expectRejected("MATCH (p:Person) CALL (p) { CALL db.labels() YIELD label } RETURN count(p)",
                   "Return statement is missing");
}

TEST_F(CallSubqueryProcedureBodyTest, namesTheMissingReturnWhenAReadFollowsTheProcedureCall) {
    expectRejected("MATCH (p:Person) CALL (p) { CALL db.labels() YIELD label MATCH (n:Person) } "
                   "RETURN count(p)",
                   "Return statement is missing");
}

// The whole query being one CALL is what a standalone call is, and it still needs no YIELD
TEST_F(CallSubqueryProcedureBodyTest, leavesAStandaloneCallAlone) {
    expectAccepted("CALL db.labels()");
    expectAccepted("CALL db.labels() YIELD label");
}
