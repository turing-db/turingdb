#include <gtest/gtest.h>

#include <string_view>

#include "TuringDB.h"

#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "columns/ColumnIDs.h"
#include "dataframe/Dataframe.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class MixedPredicateOrTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "simpledb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};

    auto query(std::string_view query, auto callback) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const QueryState state(_graphName, &_env->getMem(), &_db->getDefaultQueryConfig(), &callbacks);
        return _db->query(query, state);
    }
};

// OR-ing NodeID equalities with a property filter must not crash.
TEST_F(MixedPredicateOrTest, idOrPropertyFilter) {
    auto res = query("MATCH (n) WHERE n = 0 OR n = 1 OR n.name = 'Remy' RETURN n",
                     [](const Dataframe*) {});
    if (!res.isOk()) {
        printf("Status: %d  Error: %s\n", (int)res.getStatus(), res.getError().c_str());
    }
    EXPECT_TRUE(res);
}
