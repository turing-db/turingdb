#include <gtest/gtest.h>

#include <string_view>

#include "TuringDB.h"
#include "Graph.h"
#include "SystemManager.h"
#include "dataframe/Dataframe.h"
#include "versioning/Change.h"
#include "versioning/Transaction.h"

#include "spdlog/spdlog.h"

#include "FileUtils.h"
#include "TuringException.h"
#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

// =============================================================================
// SHORTEST PATH ERROR TESTS
// Tests designed to verify that invalid shortest path queries throw appropriate
// errors.
// =============================================================================

namespace {

const std::string_view TEST_GRAPH_CYPHER = R"(
CREATE (n1:City {name: 'CityA'}),
       (n2:City {name: 'CityB'}),
       (n3:City {name: 'CityC'}),
       (n4:City {name: 'CityD'}),
       (n1)-[:ROAD {distance: 10.0}]->(n2),
       (n2)-[:ROAD {distance: 20.0}]->(n3),
       (n1)-[:ROAD {distance: 50.0}]->(n3),
       (n3)-[:ROAD {distance: 50.0}]->(n4)
)";

const std::string_view TEST_STAR_GRAPH_CYPHER = R"(
CREATE (n1:City {name: 'CityA'}),
       (n2:City {name: 'CityB'}),
       (n3:City {name: 'CityC'}),
       (n4:City {name: 'CityD'}),
       (n2)-[:ROAD {distance: 20.0}]->(n1),
       (n3)-[:ROAD {distance: 50.0}]->(n1),
       (n4)-[:ROAD {distance: 50.0}]->(n1)
)";

class ShortestPathQueryTest : public TuringTest {
public:
    static void SetUpTestSuite() {
        _suiteOutDir = "ShortestPathQueryTest_suite.out";
        if (FileUtils::exists(_suiteOutDir)) {
            FileUtils::removeDirectory(_suiteOutDir);
        }
        FileUtils::createDirectory(_suiteOutDir);

        _env = TuringTestEnv::create(fs::Path {_suiteOutDir} / "turing");
        _db = &_env->getDB();

        // Create main test graph
        {
            _graph = _env->getSystemManager().createGraph(_graphName);

            auto changeResult = _env->getSystemManager().newChange(_graphName);
            ASSERT_TRUE(changeResult.has_value());
            Change* change = changeResult.value();
            auto changeId = change->id();

            auto status = _db->query(TEST_GRAPH_CYPHER, _graphName, &_env->getMem(),
                                     [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(status.isOk()) << "Failed to create graph: " << status.getError();

            auto submitStatus = _db->query("CHANGE SUBMIT", _graphName, &_env->getMem(),
                                           [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(submitStatus.isOk()) << "Failed to submit change: "
                                             << submitStatus.getError();
        }

        // Create star graph
        {
            _stargraph = _env->getSystemManager().createGraph(_starGraphName);

            auto changeResult = _env->getSystemManager().newChange(_starGraphName);
            ASSERT_TRUE(changeResult.has_value());
            Change* change = changeResult.value();
            auto changeId = change->id();

            auto status = _db->query(TEST_STAR_GRAPH_CYPHER, _starGraphName, &_env->getMem(),
                                     [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(status.isOk()) << "Failed to create graph: " << status.getError();

            auto submitStatus = _db->query("CHANGE SUBMIT", _starGraphName, &_env->getMem(),
                                           [](const Dataframe*) {}, CommitHash::head(), changeId);
            ASSERT_TRUE(submitStatus.isOk()) << "Failed to submit change: "
                                             << submitStatus.getError();
        }
    }

    static void TearDownTestSuite() {
        _graph = nullptr;
        _stargraph = nullptr;
        _db = nullptr;
        _env.reset();
    }

    void initialize() override {
        // Per-test setup if needed - graphs are already created in SetUpTestSuite
    }

protected:
    static inline std::string _suiteOutDir;
    static inline const std::string _graphName = "shortestpathtest";
    static inline const std::string _starGraphName = "startgraphtest";
    static inline std::unique_ptr<TuringTestEnv> _env;
    static inline TuringDB* _db = nullptr;
    static inline Graph* _graph = nullptr;
    static inline Graph* _stargraph = nullptr;

    auto query(std::string_view query, std::string_view graphName, auto callback) {
        auto res = _db->query(query, graphName, &_env->getMem(), callback,
                              CommitHash::head(), ChangeID::head());
        if (!res) {
            spdlog::error("Query failed: {}", res.getError());
        }
        return res;
    }
};

TEST_F(ShortestPathQueryTest, sucessfullTest) {
    constexpr std::string_view QUERY = "MATCH (n:City{name:'CityA'}), (m:City{name:'CityD'}) "
                                       "shortestPath(n, m, distance, dist, path) "
                                       "RETURN dist";

    bool callbackCalled = false;
    auto res = query(QUERY, _graphName, [&](const Dataframe* df) {
        callbackCalled = true;
        if (df) {
            ASSERT_EQ(df->getRowCount(), 1);
        }
    });

    EXPECT_TRUE(callbackCalled);
    EXPECT_TRUE(res.isOk());
}

TEST_F(ShortestPathQueryTest, noPathInStarGraphTest) {
    constexpr std::string_view QUERY = "MATCH (n:City{name:'CityA'}), (m) "
                                       "WHERE m.name!='CityA'"
                                       "shortestPath(n, m, distance, dist, path) "
                                       "RETURN dist";

    bool callbackCalled = false;
    auto res = query(QUERY, _starGraphName, [&](const Dataframe* df) {
        callbackCalled = true;
        if (df) {
            ASSERT_EQ(df->getRowCount(), 0);
        }
    });

    EXPECT_TRUE(callbackCalled);
    EXPECT_TRUE(res.isOk());
}

// =============================================================================
// ERROR CASE TESTS
// =============================================================================

TEST_F(ShortestPathQueryTest, unknownEdgePropertyThrowsError) {
    // Using 'nonexistent' as edge property which doesn't exist in the graph
    constexpr std::string_view QUERY = "MATCH (n:City{name:'CityA'}), (m:City{name:'CityB'}) "
                                       "shortestPath(n, m, nonexistent, dist, path) "
                                       "RETURN dist";

    bool callbackCalled = false;
    auto res = query(QUERY, _graphName, [&](const Dataframe* df) {
        callbackCalled = true;
    });

    EXPECT_FALSE(callbackCalled);
    EXPECT_FALSE(res);
    ASSERT_TRUE(res.hasErrorMessage());
    EXPECT_TRUE(res.getError().find("Unknown property") != std::string::npos);
}

TEST_F(ShortestPathQueryTest, returningInputBranchNode) {
    // Check if we throw an error when we try returning a variable of an branch
    // that is input to a ShortestPathProcessor
    constexpr std::string_view QUERY =
        "MATCH (n:City{name:'CityA'}), (m:City{name:'CityB'}) "
        "shortestPath(n, m, distance, dist, path) "
        "RETURN n";

    bool callbackCalled = false;
    auto res = query(QUERY, _graphName, [&](const Dataframe* df) {
        callbackCalled = true;
    });

    EXPECT_FALSE(callbackCalled);
    EXPECT_FALSE(res);
    ASSERT_TRUE(res.hasErrorMessage());
    EXPECT_TRUE(res.getError().find("not found in output column") != std::string::npos);
}

TEST_F(ShortestPathQueryTest, unsupportedJoinWithShortestPath) {
    // Check to ensure that a join is rejected  when one of the tips is a shortestPath processor
    constexpr std::string_view QUERY =
        "MATCH (k)-->(p), (k)-->(n:City{name:'CityA'}), (m:City{name:'CityB'}) "
        "shortestPath(n, m, distance, dist, path) "
        "RETURN n";

    bool callbackCalled = false;
    auto res = query(QUERY, _graphName, [&](const Dataframe* df) {
        callbackCalled = true;
    });

    EXPECT_FALSE(callbackCalled);
    EXPECT_FALSE(res);
    ASSERT_TRUE(res.hasErrorMessage());
    EXPECT_TRUE(res.getError().find("Common Ancestor Joins With Shortest Path Unsupported") != std::string::npos);
}

}
