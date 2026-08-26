#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "NLOutputSink.h"
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

class CallReservedWordProcedureTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        Graph* graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(graph);

        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    void runQuery(std::string_view query, NLOutputSink& sink) {
        QueryStatus status;
        _interpreter->execute(status, query, _graphName, CommitHash::head(), ChangeID::head(), &_env->getMem(), &sink);
        ASSERT_TRUE(status.isOk()) << query << ": " << status.getError();
    }

    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// CALL db.procedures() YIELD name RETURN name: the procedure's name is a keyword of the
// language (SHOW PROCEDURES), which a qualified name after the dot has to accept for the
// procedure to be callable at all.
TEST_F(CallReservedWordProcedureTest, callsAProcedureNamedByAReservedWord) {
    StringRowSink sink;
    runQuery("CALL db.procedures() YIELD name RETURN name", sink);

    std::vector<std::string> names;
    for (const StringRowSink::Row& row : sink.getRows()) {
        names.push_back(row.front());
    }

    EXPECT_TRUE(std::find(names.begin(), names.end(), "db.labels") != names.end());
    EXPECT_TRUE(std::find(names.begin(), names.end(), "db.procedures") != names.end());
}
