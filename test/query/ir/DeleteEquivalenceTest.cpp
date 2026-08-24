#include <gtest/gtest.h>

#include <algorithm>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include <range/v3/view/zip.hpp>

#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"

#include "Graph.h"
#include "QueryConfig.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

namespace rg = ranges;
namespace rv = rg::views;

class DeleteEquivalenceTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");

        SystemAccessor system = _env->getSystemManager().accessUnique();
        system.createGraph(_v2GraphName);
        system.createGraph(_v3GraphName);

        _db = &_env->getDB();
        _interp3 = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());
    }

protected:
    // Apply a write query to a graph via the v2 pipeline and submit the change.
    void applyV2(std::string_view graphName, std::string_view writeQuery) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto res = system.newChange(graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState writeState(graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus writeStatus = _db->query(writeQuery, writeState);
        ASSERT_TRUE(writeStatus.isOk()) << "V2 write failed: " << writeQuery;

        const QueryState submitState(graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus submitStatus = _db->query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "V2 CHANGE SUBMIT failed";
    }

    // Apply a write query to a graph via the v3 MLIR executor and submit the change.
    void applyV3(std::string_view graphName, std::string_view writeQuery) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto res = system.newChange(graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        NullSink sink;
        QueryStatus writeStatus;
        _interp3->execute(writeStatus, writeQuery, graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
        ASSERT_TRUE(writeStatus.isOk()) << "V3 write failed: " << writeQuery << " — " << writeStatus.getError();

        QueryCallbacks callbacks;
        callbacks.setOnOutputData([](const Dataframe*) {});

        const QueryState submitState(graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), changeID);
        const QueryStatus submitStatus = _db->query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(submitStatus.isOk()) << "V3 CHANGE SUBMIT failed";
    }

    // Run a write query via the v3 MLIR executor without asserting success, filling
    // outStatus so a caller can assert on a deliberate failure. The change is left
    // unsubmitted.
    void runV3(std::string_view graphName, std::string_view writeQuery, QueryStatus& outStatus) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            auto res = system.newChange(graphName);
            ASSERT_TRUE(res);
            changeID = res.value()->id();
        }

        NullSink sink;
        _interp3->execute(outStatus, writeQuery, graphName, CommitHash::head(), changeID, &_env->getMem(), &sink);
    }

    // Run a MATCH query via the v2 pipeline and collect results as strings.
    void matchV2(std::string_view graphName, std::string_view matchQuery, Rows& rows) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData([&rows](const Dataframe* dataframe) {
            ASSERT_TRUE(dataframe != nullptr);
            collectPipelineRows(dataframe, rows);
        });

        const QueryState state(graphName, &_env->getMem(), &_queryConfig, &callbacks);
        const QueryStatus status = _db->query(matchQuery, state);
        ASSERT_TRUE(status.isOk()) << "MATCH query failed: " << matchQuery;
    }

    // Seed both graphs identically (via v2), then apply deleteQuery via v2 on one
    // graph and via v3 on the other, and compare the MATCH read-back. Only valid for
    // cases where v2 and v3 agree (DETACH deletes, isolated-node deletes, edge deletes).
    void expectDeleteEquivalent(std::string_view seedQuery,
                                std::string_view deleteQuery,
                                std::string_view matchQuery) {
        applyV2(_v2GraphName, seedQuery);
        applyV2(_v3GraphName, seedQuery);

        applyV2(_v2GraphName, deleteQuery);
        applyV3(_v3GraphName, deleteQuery);

        Rows v2Rows;
        matchV2(_v2GraphName, matchQuery, v2Rows);
        Rows v3Rows;
        matchV2(_v3GraphName, matchQuery, v3Rows);

        std::ranges::sort(v2Rows);
        std::ranges::sort(v3Rows);

        ASSERT_EQ(v2Rows.size(), v3Rows.size())
            << "Row count mismatch for DELETE: " << deleteQuery;

        for (auto [v2Row, v3Row] : rv::zip(v2Rows, v3Rows)) {
            EXPECT_EQ(v2Row, v3Row) << "Row mismatch for DELETE: " << deleteQuery;
        }
    }

    const std::string _v2GraphName = "v2Graph";
    const std::string _v3GraphName = "v3Graph";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    std::unique_ptr<QueryInterpreterV3> _interp3;
    QueryConfig _queryConfig;
};

TEST_F(DeleteEquivalenceTest, deleteIsolatedNode) {
    expectDeleteEquivalent(
        R"(CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}))",
        R"(MATCH (n:Person {name: "Alice"}) DELETE n)",
        "MATCH (n) RETURN n.name");
}

TEST_F(DeleteEquivalenceTest, detachDeleteNodeWithEdges) {
    expectDeleteEquivalent(
        R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))",
        R"(MATCH (a:Person {name: "Alice"}) DETACH DELETE a)",
        "MATCH (n) RETURN n.name");
}

TEST_F(DeleteEquivalenceTest, detachDeleteRemovesIncidentEdge) {
    expectDeleteEquivalent(
        R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))",
        R"(MATCH (a:Person {name: "Alice"}) DETACH DELETE a)",
        "MATCH (a)-[e:KNOWS]->(b) RETURN a.name, b.name");
}

TEST_F(DeleteEquivalenceTest, deleteEdgeLeavesNodes) {
    expectDeleteEquivalent(
        R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))",
        R"(MATCH (a:Person)-[e:KNOWS]->(b:Person) DELETE e)",
        "MATCH (n) RETURN n.name");
}

TEST_F(DeleteEquivalenceTest, deleteEdgeRemovesTheEdge) {
    expectDeleteEquivalent(
        R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))",
        R"(MATCH (a:Person)-[e:KNOWS]->(b:Person) DELETE e)",
        "MATCH (a)-[e:KNOWS]->(b) RETURN a.name, b.name");
}

TEST_F(DeleteEquivalenceTest, detachDeleteMultipleNodes) {
    expectDeleteEquivalent(
        R"(CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}), (c:Person {name: "Carol"}))",
        R"(MATCH (a:Person {name: "Alice"}), (b:Person {name: "Bob"}) DETACH DELETE a, b)",
        "MATCH (n) RETURN n.name");
}

TEST_F(DeleteEquivalenceTest, detachDeleteNodeAndEdgeTogether) {
    expectDeleteEquivalent(
        R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))",
        R"(MATCH (a:Person)-[e:KNOWS]->(b:Person) DETACH DELETE a, e)",
        "MATCH (n) RETURN n.name");
}

TEST_F(DeleteEquivalenceTest, detachDeleteAllNodes) {
    expectDeleteEquivalent(
        R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))",
        R"(MATCH (n) DETACH DELETE n)",
        "MATCH (n) RETURN n.name");
}

TEST_F(DeleteEquivalenceTest, plainDeleteOfConnectedNodeFails) {
    applyV2(_v3GraphName, R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))");

    QueryStatus status;
    runV3(_v3GraphName, R"(MATCH (a:Person {name: "Alice"}) DELETE a)", status);

    EXPECT_FALSE(status.isOk()) << "plain DELETE of a connected node should fail in v3";
}

TEST_F(DeleteEquivalenceTest, detachDeleteOfConnectedNodeSucceeds) {
    applyV2(_v3GraphName, R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))");

    QueryStatus status;
    runV3(_v3GraphName, R"(MATCH (a:Person {name: "Alice"}) DETACH DELETE a)", status);

    EXPECT_TRUE(status.isOk()) << "DETACH DELETE of a connected node should succeed in v3";
}
