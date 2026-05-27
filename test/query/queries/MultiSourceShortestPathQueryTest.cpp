#include <gtest/gtest.h>

#include <string_view>

#include "TuringDB.h"
#include "QueryConfig.h"
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

class MultiSourceShortestPathQueryTest : public TuringTest {
public:
    static auto query(std::string_view query, std::string_view graphName, auto callback,
                      ChangeID change = ChangeID::head()) {
        QueryCallbacks callbacks;
        callbacks.setOnOutputData(callback);
        const QueryState state(graphName, &_env->getMem(), &_queryConfig, &callbacks, CommitHash::head(), change);
        auto res = _db->query(query, state);
        if (!res) {
            spdlog::error("Query failed: {}", res.getError());
        }
        return res;
    }

    static void SetUpTestSuite() {
        _suiteOutDir = "MultiSourceShortestPathQueryTest_suite.out";
        if (FileUtils::exists(_suiteOutDir)) {
            FileUtils::removeDirectory(_suiteOutDir);
        }
        FileUtils::createDirectory(_suiteOutDir);

        _env = TuringTestEnv::create(fs::Path {_suiteOutDir} / "turing");
        _db = &_env->getDB();

        {
            _graph = _env->getSystemManager().createGraph(_graphName);

            auto changeResult = _env->getSystemManager().newChange(_graphName);
            ASSERT_TRUE(changeResult.has_value());
            Change* change = changeResult.value();
            auto changeId = change->id();

            auto status = query(TEST_GRAPH_CYPHER, _graphName, [](const Dataframe*) {}, changeId);
            ASSERT_TRUE(status.isOk()) << "Failed to create graph: " << status.getError();

            auto submitStatus = query("CHANGE SUBMIT", _graphName, [](const Dataframe*) {}, changeId);
            ASSERT_TRUE(submitStatus.isOk()) << "Failed to submit change: "
                                             << submitStatus.getError();
        }

        {
            _stargraph = _env->getSystemManager().createGraph(_starGraphName);

            auto changeResult = _env->getSystemManager().newChange(_starGraphName);
            ASSERT_TRUE(changeResult.has_value());
            Change* change = changeResult.value();
            auto changeId = change->id();

            auto status = query(TEST_STAR_GRAPH_CYPHER, _starGraphName, [](const Dataframe*) {}, changeId);
            ASSERT_TRUE(status.isOk()) << "Failed to create graph: " << status.getError();

            auto submitStatus = query("CHANGE SUBMIT", _starGraphName, [](const Dataframe*) {}, changeId);
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
    }

protected:
    static inline std::string _suiteOutDir;
    static inline const std::string _graphName = "mssptest";
    static inline const std::string _starGraphName = "msspstargraphtest";
    static inline std::unique_ptr<TuringTestEnv> _env;
    static inline TuringDB* _db = nullptr;
    static inline Graph* _graph = nullptr;
    static inline Graph* _stargraph = nullptr;
    static inline QueryConfig _queryConfig;
};

TEST_F(MultiSourceShortestPathQueryTest, basicMultiSourceTest) {
    constexpr std::string_view QUERY =
        "MATCH (n:City), (m:City) "
        "WHERE n.name = 'CityA' OR n.name = 'CityB' "
        "multiSourceShortestPath(n, m, distance, src, tgt, dist, path) "
        "RETURN src, tgt, dist, path";

    bool callbackCalled = false;
    auto res = query(QUERY, _graphName, [&](const Dataframe* df) {
        callbackCalled = true;
        if (df) {
            ASSERT_GT(df->getLogicalRowCount(), 0);
        }
    });

    EXPECT_TRUE(callbackCalled);
    EXPECT_TRUE(res.isOk()) << res.getError();
}

TEST_F(MultiSourceShortestPathQueryTest, singleSourceTest) {
    constexpr std::string_view QUERY =
        "MATCH (n:City{name:'CityA'}), (m:City{name:'CityD'}) "
        "multiSourceShortestPath(n, m, distance, src, tgt, dist, path) "
        "RETURN dist";

    bool callbackCalled = false;
    auto res = query(QUERY, _graphName, [&](const Dataframe* df) {
        callbackCalled = true;
        if (df) {
            ASSERT_EQ(df->getLogicalRowCount(), 1);
        }
    });

    EXPECT_TRUE(callbackCalled);
    EXPECT_TRUE(res.isOk()) << res.getError();
}

TEST_F(MultiSourceShortestPathQueryTest, noPathInStarGraphTest) {
    constexpr std::string_view QUERY =
        "MATCH (n:City{name:'CityA'}), (m) "
        "WHERE m.name!='CityA' "
        "multiSourceShortestPath(n, m, distance, src, tgt, dist, path) "
        "RETURN dist";

    bool callbackCalled = false;
    auto res = query(QUERY, _starGraphName, [&](const Dataframe* df) {
        callbackCalled = true;
        if (df) {
            ASSERT_EQ(df->getLogicalRowCount(), 0);
        }
    });

    EXPECT_TRUE(callbackCalled);
    EXPECT_TRUE(res.isOk()) << res.getError();
}

TEST_F(MultiSourceShortestPathQueryTest, unknownEdgePropertyThrowsError) {
    constexpr std::string_view QUERY =
        "MATCH (n:City{name:'CityA'}), (m:City{name:'CityB'}) "
        "multiSourceShortestPath(n, m, nonexistent, src, tgt, dist, path) "
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

}
