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

// A body that reads and ends on no clause at all writes nothing: what it is missing is its
// RETURN, and that is what the error names whatever clause follows the CALL
class CallSubqueryReadOnlyBodyTest : public TuringTest {
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

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(CallSubqueryReadOnlyBodyTest, namesTheMissingReturnOfAReadOnlyBody) {
    expectRejected("CALL { MATCH (n) } RETURN 1", "Return statement is missing");
    expectRejected("CALL { MATCH (n) } MATCH (m) RETURN count(m)", "Return statement is missing");
}

TEST_F(CallSubqueryReadOnlyBodyTest, namesTheMissingReturnOfAnImportingReadOnlyBody) {
    expectRejected("MATCH (p:Person) CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) } RETURN count(p)",
                   "Return statement is missing");
    expectRejected("MATCH (p:Person) CALL (p) { MATCH (p)-[:INTERESTED_IN]->(i) } "
                   "MATCH (q:Person) RETURN count(q)",
                   "Return statement is missing");
}
