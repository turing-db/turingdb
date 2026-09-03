#include <gtest/gtest.h>

#include <memory>
#include <span>
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

#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace {

// Discards output — the queries under test are rejected before producing rows.
class NullSink : public NLOutputSink {
public:
    void appendChunks(std::span<const Column* const>, size_t, size_t) override {}
};

}

class QueryInterpreterV3ErrorTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, QueryStatus& status) {
        NullSink sink;

        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// An internal generator failure is a FatalException and must keep reading as
// one, rather than being dressed up as a deliberate rejection. A map literal
// reading a row is one: the key varies, so it is not dropped as a constant, and
// the generator has no column to read a map into.
TEST_F(QueryInterpreterV3ErrorTest, reportsInternalGeneratorFailureAsUnexpected) {
    QueryStatus status;
    runQuery("MATCH (n) RETURN n.name ORDER BY {a: n.age}", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::PLAN_ERROR);
    EXPECT_EQ(status.getError(), "Unexpected exception: Unsupported literal kind in WHERE clause expression.");
}

TEST_F(QueryInterpreterV3ErrorTest, deleteOutsideWrite) {
    QueryStatus status;
    runQuery("MATCH (n) DELETE n", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_EQ(status.getError(), "Cannot perform DELETE outside of a write transaction.");
}

TEST_F(QueryInterpreterV3ErrorTest, setOutsideWrite) {
    QueryStatus status;
    runQuery("MATCH (n) SET n.age = 0", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_EQ(status.getError(), "Cannot perform SET outside of a write transaction.");
}

TEST_F(QueryInterpreterV3ErrorTest, createOutsideWrite) {
    QueryStatus status;
    runQuery("MATCH (n) CREATE (m:M)", status);

    EXPECT_EQ(status.getStatus(), QueryStatus::Status::EXEC_ERROR);
    EXPECT_EQ(status.getError(), "Cannot perform CREATE outside of a write transaction.");
}

