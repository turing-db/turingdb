#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

#include "Graph.h"
#include "TuringDB.h"
#include "QueryConfig.h"
#include "SystemManager.h"
#include "SimpleGraph.h"

#include "TuringTestEnv.h" // pulls in `using namespace db;`
#include "TuringTest.h"

#include "dataframe/Dataframe.h"
#include "reader/GraphReader.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "versioning/Transaction.h"

namespace turing::test {

// Shared fixture for tests that run Cypher against an in-process graph.
//
// By default it creates a graph named "simpledb" populated with the shared
// SimpleGraph fixture. Subclasses customise by overriding:
//   - graphName()     — the graph name (also the query target)
//   - populateGraph() — the fixture data (e.g. leave empty, or load a custom graph)
class GraphQueryTest : public TuringTest {
public:
    void initialize() override {
        _graphName = std::string(graphName());
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        SystemAccessor system = _env->getSystemManager().accessUnique();
        _graph = system.createGraph(_graphName);
        populateGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    virtual std::string_view graphName() const { return "simpledb"; }

    virtual void populateGraph(Graph* graph) { SimpleGraph::createSimpleGraph(graph); }

    // Run `q` against the current change.
    auto query(std::string_view q, auto callback) { return query(q, callback, _currentChange); }

    // Run `q` in an explicit change (ChangeID::head() = main).
    auto query(std::string_view q, auto callback, ChangeID change) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks,
                               CommitHash::head(), change);
        return _db->query(q, state);
    }

    // Convenience for tests that only care about the status, not the output.
    auto query(std::string_view q) {
        return query(q, [](const Dataframe*) {});
    }

    GraphReader read() { return _graph->openTransaction().readGraph(); }

    void newChange() {
        SystemAccessor system = _env->getSystemManager().accessUnique();
        auto res = system.newChange(_graphName);
        ASSERT_TRUE(res);
        _currentChange = res.value()->id();
    }

    void submitCurrentChange() {
        const auto res = query("CHANGE SUBMIT", [](const Dataframe*) {});
        ASSERT_TRUE(res.isOk());
        _currentChange = ChangeID::head();
    }

    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    Graph* _graph {nullptr};
    std::string _graphName {"simpledb"};
    QueryConfig _queryConfig;
    ChangeID _currentChange {ChangeID::head()};
};

} // namespace turing::test
