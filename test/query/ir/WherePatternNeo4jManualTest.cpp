#include <gtest/gtest.h>

#include <stdint.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// WHERE inside node and relationship patterns, over the graph and the rows of the Neo4j
// manual's WHERE page:
// https://neo4j.com/docs/cypher-manual/current/clauses/where/#fixed-length-patterns
class WherePatternNeo4jManualTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

    // The page's CREATE, node for node:
    //
    //   CREATE (andy:Swedish:Person {name: 'Andy', age: 36}),
    //          (timothy:Person {name: 'Timothy', age: 38}),
    //          (peter:Person {name: 'Peter', age: 35}),
    //          (lisa:Person {name: 'Lisa', age: 48}),
    //          (john:Person {name: 'John', age: 40}),
    //          (susan:Person {name: 'Susan', age: 32}),
    //          (andy)-[:KNOWS {since: 2012}]->(timothy),
    //          (andy)-[:KNOWS {since: 1999}]->(peter),
    //          (peter)-[:KNOWS {since: 2005}]->(lisa),
    //          (lisa)-[:KNOWS {since: 2010}]->(john),
    //          (john)-[:KNOWS {since: 2021}]->(susan)
    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const NodeID andy = writer.addNode({"Swedish", "Person"});
        writer.addNodeProperty<types::String>(andy, "name", "Andy");
        writer.addNodeProperty<types::Int64>(andy, "age", 36);

        const NodeID timothy = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(timothy, "name", "Timothy");
        writer.addNodeProperty<types::Int64>(timothy, "age", 38);

        const NodeID peter = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(peter, "name", "Peter");
        writer.addNodeProperty<types::Int64>(peter, "age", 35);

        const NodeID lisa = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(lisa, "name", "Lisa");
        writer.addNodeProperty<types::Int64>(lisa, "age", 48);

        const NodeID john = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(john, "name", "John");
        writer.addNodeProperty<types::Int64>(john, "age", 40);

        const NodeID susan = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(susan, "name", "Susan");
        writer.addNodeProperty<types::Int64>(susan, "age", 32);

        const EdgeRecord andyTimothy = writer.addEdge("KNOWS", andy, timothy);
        writer.addEdgeProperty<types::Int64>(andyTimothy, "since", 2012);

        const EdgeRecord andyPeter = writer.addEdge("KNOWS", andy, peter);
        writer.addEdgeProperty<types::Int64>(andyPeter, "since", 1999);

        const EdgeRecord peterLisa = writer.addEdge("KNOWS", peter, lisa);
        writer.addEdgeProperty<types::Int64>(peterLisa, "since", 2005);

        const EdgeRecord lisaJohn = writer.addEdge("KNOWS", lisa, john);
        writer.addEdgeProperty<types::Int64>(lisaJohn, "since", 2010);

        const EdgeRecord johnSusan = writer.addEdge("KNOWS", john, susan);
        writer.addEdgeProperty<types::Int64>(johnSusan, "since", 2021);

        writer.submit();
        jobSystem.terminate();
    }

    void runQuery(QueryStatus& status, std::string_view query, RowSink* sink) {
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              sink);
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        runQuery(status, query, &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectError(std::string_view query, std::string_view message) {
        RowSink sink;
        QueryStatus status;
        runQuery(status, query, &sink);

        ASSERT_FALSE(status.isOk()) << "query: " << query << " was expected to fail";
        EXPECT_NE(status.getError().find(message), std::string::npos)
            << "query: " << query << "\nerror: " << status.getError();
    }

    const std::string _graphName = "neo4jmanual";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(WherePatternNeo4jManualTest, whereInsideNodePattern) {
    expectRows("WITH 35 AS minAge "
               "MATCH (a:Person WHERE a.name = 'Andy')-[:KNOWS]->(b:Person WHERE b.age > minAge) "
               "RETURN b.name AS name",
               {{"Timothy"}});
}

TEST_F(WherePatternNeo4jManualTest, whereInsidePatternComprehension) {
    GTEST_SKIP() << "Pattern comprehensions have no rule in the grammar";

    expectRows("MATCH (a:Person {name: 'Andy'}) "
               "RETURN [(a)-->(b WHERE b:Person) | b.name] AS friends",
               {{"[\"Peter\", \"Timothy\"]"}});
}

TEST_F(WherePatternNeo4jManualTest, whereInsideRelationshipPattern) {
    expectRows("WITH 2000 AS minYear "
               "MATCH (a:Person)-[r:KNOWS WHERE r.since < minYear]->(b:Person) "
               "RETURN a.name AS person, b.name AS friend, r.since AS knowsSince",
               {{"Andy", "Peter", "1999"}});
}

TEST_F(WherePatternNeo4jManualTest, relationshipPredicateInPatternComprehension) {
    GTEST_SKIP() << "Pattern comprehensions have no rule in the grammar";

    expectRows("WITH 2000 AS minYear "
               "MATCH (a:Person {name: 'Andy'}) "
               "RETURN [(a)-[r:KNOWS WHERE r.since < minYear]->(b:Person) | r.since] AS years",
               {{"[1999]"}});
}

TEST_F(WherePatternNeo4jManualTest, whereInsideSingleNodePattern) {
    expectRows("MATCH (a:Person WHERE a.age > 36) RETURN a.name",
               {{"Timothy"}, {"Lisa"}, {"John"}});
}

TEST_F(WherePatternNeo4jManualTest, inlineAndTrailingWhere) {
    expectRows("MATCH (a:Person WHERE a.age > 35)-[:KNOWS]->(b) "
               "WHERE b.age < 40 "
               "RETURN a.name, b.name",
               {{"Andy", "Timothy"}, {"Andy", "Peter"}, {"John", "Susan"}});
}

TEST_F(WherePatternNeo4jManualTest, whereInsideTwoHopPattern) {
    expectRows("MATCH (a:Person)-[:KNOWS]->(b WHERE b.age < 36)-[r:KNOWS WHERE r.since > 2000]->(c) "
               "RETURN a.name, b.name, c.name",
               {{"Andy", "Peter", "Lisa"}});
}

TEST_F(WherePatternNeo4jManualTest, whereInsideOptionalMatch) {
    expectRows("MATCH (a:Person) "
               "OPTIONAL MATCH (a)-[r:KNOWS WHERE r.since > 2010]->(b) "
               "RETURN a.name, b.name",
               {{"Andy", "Timothy"},
                {"Timothy", "null"},
                {"Peter", "null"},
                {"Lisa", "null"},
                {"John", "Susan"},
                {"Susan", "null"}});
}

TEST_F(WherePatternNeo4jManualTest, whereInsideExistsPattern) {
    expectRows("MATCH (a:Person) "
               "WHERE EXISTS { (a)-[:KNOWS]->(b WHERE b.age > 45) } "
               "RETURN a.name",
               {{"Peter"}});
}

TEST_F(WherePatternNeo4jManualTest, readsVariableOfEarlierMatch) {
    expectRows("MATCH (a:Person {name: 'Andy'}) "
               "MATCH (a)-[:KNOWS]->(b:Person WHERE b.age < a.age) "
               "RETURN b.name",
               {{"Peter"}});
}

TEST_F(WherePatternNeo4jManualTest, readsLaterElementOfSamePattern) {
    expectError("MATCH (a:Person WHERE a.age > b.age)-[:KNOWS]->(b:Person) RETURN a.name",
                "cannot reference 'b'");
}

TEST_F(WherePatternNeo4jManualTest, readsEarlierElementOfSamePattern) {
    expectError("MATCH (a:Person)-[r:KNOWS WHERE a.age > 30]->(b:Person) RETURN a.name",
                "cannot reference 'a'");
}

TEST_F(WherePatternNeo4jManualTest, readsElementOfOtherPatternPart) {
    expectError("MATCH (a:Person), (b:Person WHERE b.age = a.age) RETURN b.name",
                "cannot reference 'a'");
}

TEST_F(WherePatternNeo4jManualTest, nonBooleanPredicate) {
    expectError("MATCH (a:Person WHERE a.age) RETURN a.name",
                "WHERE expression must be a boolean");
}

TEST_F(WherePatternNeo4jManualTest, whereInsideCreatePattern) {
    expectError("CREATE (a:Person WHERE a.age > 30)",
                "WHERE is not allowed in a CREATE pattern");
}

TEST_F(WherePatternNeo4jManualTest, whereInsideMergePattern) {
    expectError("MERGE (a:Person WHERE a.age > 30)",
                "WHERE is not allowed in a MERGE pattern");
}
