#include <gtest/gtest.h>

#include <string>

#include "TuringDB.h"
#include "QueryConfig.h"
#include "Graph.h"
#include "SimpleGraph.h"
#include "SystemManager.h"
#include "QueryStatus.h"
#include "dataframe/Dataframe.h"

#include "TuringTestEnv.h"
#include "TuringTest.h"

using namespace turing::test;

class IncorrectQueryTest : public TuringTest {
public:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _graph = _env->getSystemManager().createGraph(_graphName);
        SimpleGraph::createSimpleGraph(_graph);
        _db = &_env->getDB();
    }

protected:
    const std::string _graphName = "testdb";
    std::unique_ptr<TuringTestEnv> _env;
    TuringDB* _db {nullptr};
    QueryConfig _queryConfig;
    Graph* _graph {nullptr};

    QueryStatus query(std::string_view q) {
        QueryCallbacks callbacks;
        const QueryState state(_graphName, &_env->getMem(), &_queryConfig, &callbacks);
        return _db->query(q, state);
    }
};

TEST_F(IncorrectQueryTest, missingClosingParenthesis) {
    auto result = query("MATCH (n RETURN n");

    EXPECT_FALSE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::PARSE_ERROR);
    EXPECT_TRUE(result.hasErrorMessage());
}

TEST_F(IncorrectQueryTest, completelyInvalidSyntax) {
    auto result = query("THIS IS NOT CYPHER");

    EXPECT_FALSE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::PARSE_ERROR);
    EXPECT_TRUE(result.hasErrorMessage());
}

TEST_F(IncorrectQueryTest, emptyQuery) {
    auto result = query("");

    EXPECT_FALSE(result);
    EXPECT_TRUE(result.hasErrorMessage());
}

TEST_F(IncorrectQueryTest, undefinedVariable) {
    auto result = query("MATCH (n) WHERE m.age > 10 RETURN n");

    EXPECT_FALSE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::ANALYZE_ERROR);
    EXPECT_TRUE(result.hasErrorMessage());
}

TEST_F(IncorrectQueryTest, returnUndefinedVariable) {
    auto result = query("MATCH (n) RETURN x");

    EXPECT_FALSE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::ANALYZE_ERROR);
    EXPECT_TRUE(result.hasErrorMessage());
}

TEST_F(IncorrectQueryTest, missingReturnClause) {
    auto result = query("MATCH (n)");

    EXPECT_FALSE(result);
    EXPECT_TRUE(result.hasErrorMessage());
}

TEST_F(IncorrectQueryTest, invalidRelationshipSyntax) {
    auto result = query("MATCH (n)-[)->(m) RETURN n");

    EXPECT_FALSE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::PARSE_ERROR);
    EXPECT_TRUE(result.hasErrorMessage());
}

TEST_F(IncorrectQueryTest, validQuerySucceeds) {
    auto result = query("MATCH (n) RETURN n");

    EXPECT_TRUE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::OK);
}

TEST_F(IncorrectQueryTest, queryNonExistentGraph) {
    QueryCallbacks callbacks;
    const QueryState state("no_such_graph", &_env->getMem(), &_queryConfig, &callbacks);
    auto result = _db->query("MATCH (n) RETURN n", state);

    EXPECT_FALSE(result);
    EXPECT_EQ(result.getStatus(), QueryStatus::Status::GRAPH_NOT_FOUND);
}
