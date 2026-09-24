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

// A bare pattern in an expression, `WHERE (a)-[:KNOWS]->(b)`, is true for a row when the
// pattern matches from it, as `EXISTS { (a)-[:KNOWS]->(b) }` is. Over the graph of the Neo4j
// manual's WHERE page.
class PatternPredicateTest : public TuringTest {
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

TEST_F(PatternPredicateTest, betweenBoundNodes) {
    expectRows("MATCH (a:Person), (b:Person) WHERE (a)-[:KNOWS]->(b) RETURN a.name, b.name",
               {{"Andy", "Timothy"},
                {"Andy", "Peter"},
                {"Peter", "Lisa"},
                {"Lisa", "John"},
                {"John", "Susan"}});
}

TEST_F(PatternPredicateTest, negated) {
    expectRows("MATCH (p:Person) WHERE NOT (p)-[:KNOWS]->() RETURN p.name",
               {{"Timothy"}, {"Susan"}});
}

TEST_F(PatternPredicateTest, backwardToLabelledAnonymousNode) {
    expectRows("MATCH (p:Person) WHERE (p)<-[:KNOWS]-(:Person {name: 'Andy'}) RETURN p.name",
               {{"Timothy"}, {"Peter"}});
}

TEST_F(PatternPredicateTest, undirected) {
    expectRows("MATCH (p:Person) WHERE (p)-[:KNOWS]-({name: 'Lisa'}) RETURN p.name",
               {{"Peter"}, {"John"}});
}

TEST_F(PatternPredicateTest, rootWithLabelsAndProperties) {
    expectRows("MATCH (p:Person) WHERE (:Person {name: 'Andy'})-[:KNOWS]->(p) RETURN p.name",
               {{"Timothy"}, {"Peter"}});
}

TEST_F(PatternPredicateTest, rootWithPropertiesOnly) {
    expectRows("MATCH (p:Person) WHERE ({name: 'Andy'})-[:KNOWS]->(p) RETURN p.name",
               {{"Timothy"}, {"Peter"}});
}

TEST_F(PatternPredicateTest, rootWithVariableAndLabel) {
    expectRows("MATCH (p) WHERE (p:Person)-[:KNOWS]->(:Person {name: 'Lisa'}) RETURN p.name",
               {{"Peter"}});
}

TEST_F(PatternPredicateTest, rootWithVariableAndProperties) {
    expectRows("MATCH (p:Person) WHERE (p {age: 35})-[:KNOWS]->() RETURN p.name",
               {{"Peter"}});
}

TEST_F(PatternPredicateTest, twoHops) {
    expectRows("MATCH (p:Person) "
               "WHERE (p)-[:KNOWS]->(:Person {name: 'Peter'})-[:KNOWS]->(:Person {name: 'Lisa'}) "
               "RETURN p.name",
               {{"Andy"}});
}

TEST_F(PatternPredicateTest, threeHops) {
    expectRows("MATCH (p:Person) "
               "WHERE (p)-[:KNOWS]->({name: 'Lisa'})-[:KNOWS]->({name: 'John'})-[:KNOWS]->({name: 'Susan'}) "
               "RETURN p.name",
               {{"Peter"}});
}

TEST_F(PatternPredicateTest, conjunctionWithComparison) {
    expectRows("MATCH (p:Person) WHERE p.age > 35 AND (p)-[:KNOWS]->() RETURN p.name",
               {{"Andy"}, {"Lisa"}, {"John"}});
}

TEST_F(PatternPredicateTest, disjunctionOfPatterns) {
    expectRows("MATCH (p:Person) "
               "WHERE (p)-[:KNOWS]->({name: 'Susan'}) OR (p)<-[:KNOWS]-({name: 'Andy'}) "
               "RETURN p.name",
               {{"John"}, {"Timothy"}, {"Peter"}});
}

TEST_F(PatternPredicateTest, relationshipProperties) {
    expectRows("MATCH (p:Person) WHERE (p)-[:KNOWS {since: 2005}]->() RETURN p.name",
               {{"Peter"}});
}

TEST_F(PatternPredicateTest, inReturn) {
    expectRows("MATCH (p:Person) RETURN p.name, (p)-[:KNOWS]->() AS knows",
               {{"Andy", "true"},
                {"Timothy", "false"},
                {"Peter", "true"},
                {"Lisa", "true"},
                {"John", "true"},
                {"Susan", "false"}});
}

TEST_F(PatternPredicateTest, introducesNodeVariable) {
    expectError("MATCH (p:Person) WHERE (p)-[:KNOWS]->(q) RETURN p.name",
                "cannot introduce new variables: 'q'");
}

TEST_F(PatternPredicateTest, introducesEdgeVariable) {
    expectError("MATCH (p:Person) WHERE (p)-[r:KNOWS]->() RETURN p.name",
                "cannot introduce new variables: 'r'");
}
