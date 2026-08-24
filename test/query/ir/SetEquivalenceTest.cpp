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

class SetEquivalenceTest : public TuringTest {
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

    // Seed both graphs identically (via v2), then apply setQuery via v2 on one graph
    // and via v3 on the other, and finally compare the MATCH read-back. Seeding both
    // sides the same way isolates the SET behaviour as the only difference.
    void expectSetEquivalent(std::string_view seedQuery,
                             std::string_view setQuery,
                             std::string_view matchQuery) {
        applyV2(_v2GraphName, seedQuery);
        applyV2(_v3GraphName, seedQuery);

        applyV2(_v2GraphName, setQuery);
        applyV3(_v3GraphName, setQuery);

        Rows v2Rows;
        matchV2(_v2GraphName, matchQuery, v2Rows);
        Rows v3Rows;
        matchV2(_v3GraphName, matchQuery, v3Rows);

        std::ranges::sort(v2Rows);
        std::ranges::sort(v3Rows);

        ASSERT_EQ(v2Rows.size(), v3Rows.size())
            << "Row count mismatch for SET: " << setQuery;

        for (auto [v2Row, v3Row] : rv::zip(v2Rows, v3Rows)) {
            EXPECT_EQ(v2Row, v3Row) << "Row mismatch for SET: " << setQuery;
        }
    }

    const std::string _v2GraphName = "v2Graph";
    const std::string _v3GraphName = "v3Graph";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    std::unique_ptr<QueryInterpreterV3> _interp3;
    QueryConfig _queryConfig;
};

TEST_F(SetEquivalenceTest, setNewNodeProperty) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice"}))",
        R"(MATCH (n:Person) SET n.age = 32)",
        "MATCH (n:Person) RETURN n.name, n.age");
}

TEST_F(SetEquivalenceTest, overwriteExistingNodeProperty) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice", age: 10}))",
        R"(MATCH (n:Person) SET n.age = 32)",
        "MATCH (n:Person) RETURN n.age");
}

TEST_F(SetEquivalenceTest, setStringProperty) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice"}))",
        R"(MATCH (n:Person) SET n.city = "Paris")",
        "MATCH (n:Person) RETURN n.city");
}

TEST_F(SetEquivalenceTest, multipleItemsInOneSet) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice"}))",
        R"(MATCH (n:Person) SET n.age = 32, n.city = "Paris")",
        "MATCH (n:Person) RETURN n.age, n.city");
}

TEST_F(SetEquivalenceTest, chainedSetStatements) {
    expectSetEquivalent(
        R"(CREATE (n:Person {name: "Alice"}))",
        R"(MATCH (n:Person) SET n.age = 1 SET n.age = 2)",
        "MATCH (n:Person) RETURN n.age");
}

TEST_F(SetEquivalenceTest, setAcrossMultipleMatchedNodes) {
    expectSetEquivalent(
        R"(CREATE (a:Person {name: "Alice"}), (b:Person {name: "Bob"}))",
        R"(MATCH (n:Person) SET n.age = 40)",
        "MATCH (n:Person) RETURN n.name, n.age");
}

TEST_F(SetEquivalenceTest, setFromAnotherProperty) {
    expectSetEquivalent(
        R"(CREATE (a:Person {name: "Alice", age: 10}), (b:Person {name: "Bob", age: 20}))",
        R"(MATCH (n:Person) SET n.years = n.age)",
        "MATCH (n:Person) RETURN n.name, n.years");
}

TEST_F(SetEquivalenceTest, setOnlyMatchedLabel) {
    expectSetEquivalent(
        R"(CREATE (a:Person {name: "Alice", rank: 1}), (b:Robot {name: "R2", rank: 99}))",
        R"(MATCH (n:Person) SET n.rank = 5)",
        "MATCH (n) RETURN n.name, n.rank");
}

TEST_F(SetEquivalenceTest, setEdgeProperty) {
    expectSetEquivalent(
        R"(CREATE (a:Person {name: "Alice"})-[:KNOWS]->(b:Person {name: "Bob"}))",
        R"(MATCH (a:Person)-[e:KNOWS]->(b:Person) SET e.since = 1999)",
        "MATCH (a:Person)-[e:KNOWS]->(b:Person) RETURN e.since");
}

TEST_F(SetEquivalenceTest, setAcrossCrossProduct) {
    expectSetEquivalent(
        R"(CREATE (:Person {name: "a"}), (:Person {name: "b"}), (:Robot), (:Robot), (:Robot))",
        "MATCH (p:Person), (r:Robot) SET p.rank = 7",
        "MATCH (p:Person) RETURN p.name, p.rank");
}
