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

// AFL inputs that tripped the 'Component var not registered' assertion of DBProgramGenerator.
class FuzzComponentVarNotRegisteredTest : public TuringTest {
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

    void expectNoRows(std::string_view query) {
        RowSink sink;
        const QueryStatus status = runQuery(query, &sink);
        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_TRUE(sink.rows().empty()) << "query: " << query << "\nactual:\n" << actualText;
    }

    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    std::string _graphName {"simpledb"};
};

TEST_F(FuzzComponentVarNotRegisteredTest, Match000054) {
    expectNoRows("MATCH (y)-->(z)-->(m),(n), (l)-->(m), (n)-->(p)-->(m), (n)-->(p)-->(),(n), (y)-->(m), (n)-->(p)-->(n), (n)-->(p)-- (y)-->(q) WHERE m.age < p.age  RETURN n,m,z,p");
}

TEST_F(FuzzComponentVarNotRegisteredTest, Match000107) {
    expectNoRows("MATCH (y)-->(z)-->(m),(m), (n)-->(m), (n)-->(p)-->(m), (n)-->(p)-->(),(ny)-->(m), (n)-->(p)-->(m), (y), (y)-->(m), (n)-->(q)-->(n), (y)-->(q) WHERE m.age                                < p.age  RETURN n,m,z,p");
}
