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

// Every example of the Neo4j manual's EXISTS subqueries page, with the graph that page
// builds and the rows it says each query answers:
// https://neo4j.com/docs/cypher-manual/current/subqueries/existential/
//
// The graph is the page's own rather than the shared SimpleGraph because the expected rows
// are the page's: they only mean anything against the nodes it creates.
//
// Two examples reach a clause the engine does not have. Each skips with what is missing and
// keeps the query and the rows the manual documents, so the test is what there is to make
// pass once the clause lands.
class ExistsNeo4jManualTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

    // The page's CREATE, node for node:
    //
    //   CREATE
    //   (andy:Swedish:Person {name: 'Andy', age: 36}),
    //   (timothy:Person {name: 'Timothy', nickname: 'Tim', age: 25}),
    //   (peter:Person {name: 'Peter', nickname: 'Pete', age: 35}),
    //   (andy)-[:HAS_DOG {since: 2016}]->(:Dog {name:'Andy'}),
    //   (timothy)-[:HAS_CAT {since: 2019}]->(:Cat {name:'Mittens'}),
    //   (fido:Dog {name:'Fido'})<-[:HAS_DOG {since: 2010}]-(peter)-[:HAS_DOG {since: 2018}]->(:Dog {name:'Ozzy'}),
    //   (fido)-[:HAS_TOY]->(:Toy{name:'Banana'})
    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const NodeID andy = writer.addNode({"Swedish", "Person"});
        writer.addNodeProperty<types::String>(andy, "name", "Andy");
        writer.addNodeProperty<types::Int64>(andy, "age", 36);

        const NodeID timothy = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(timothy, "name", "Timothy");
        writer.addNodeProperty<types::String>(timothy, "nickname", "Tim");
        writer.addNodeProperty<types::Int64>(timothy, "age", 25);

        const NodeID peter = writer.addNode({"Person"});
        writer.addNodeProperty<types::String>(peter, "name", "Peter");
        writer.addNodeProperty<types::String>(peter, "nickname", "Pete");
        writer.addNodeProperty<types::Int64>(peter, "age", 35);

        const NodeID andysDog = writer.addNode({"Dog"});
        writer.addNodeProperty<types::String>(andysDog, "name", "Andy");

        const NodeID mittens = writer.addNode({"Cat"});
        writer.addNodeProperty<types::String>(mittens, "name", "Mittens");

        const NodeID fido = writer.addNode({"Dog"});
        writer.addNodeProperty<types::String>(fido, "name", "Fido");

        const NodeID ozzy = writer.addNode({"Dog"});
        writer.addNodeProperty<types::String>(ozzy, "name", "Ozzy");

        const NodeID banana = writer.addNode({"Toy"});
        writer.addNodeProperty<types::String>(banana, "name", "Banana");

        const EdgeRecord andyHasDog = writer.addEdge("HAS_DOG", andy, andysDog);
        writer.addEdgeProperty<types::Int64>(andyHasDog, "since", 2016);

        const EdgeRecord timothyHasCat = writer.addEdge("HAS_CAT", timothy, mittens);
        writer.addEdgeProperty<types::Int64>(timothyHasCat, "since", 2019);

        const EdgeRecord peterHasFido = writer.addEdge("HAS_DOG", peter, fido);
        writer.addEdgeProperty<types::Int64>(peterHasFido, "since", 2010);

        const EdgeRecord peterHasOzzy = writer.addEdge("HAS_DOG", peter, ozzy);
        writer.addEdgeProperty<types::Int64>(peterHasOzzy, "since", 2018);

        writer.addEdge("HAS_TOY", fido, banana);

        writer.submit();
        jobSystem.terminate();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    const std::string _graphName = "neo4jmanual";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

TEST_F(ExistsNeo4jManualTest, simpleExistsSubquery) {
    expectRows("MATCH (person:Person) "
               "WHERE EXISTS { "
               "    (person)-[:HAS_DOG]->(:Dog) "
               "} "
               "RETURN person.name AS name",
               {{"Andy"}, {"Peter"}});
}

TEST_F(ExistsNeo4jManualTest, existsSubqueryWithWhereClause) {
    expectRows("MATCH (person:Person) "
               "WHERE EXISTS { "
               "  MATCH (person)-[:HAS_DOG]->(dog:Dog) "
               "  WHERE person.name = dog.name "
               "} "
               "RETURN person.name AS name",
               {{"Andy"}});
}

TEST_F(ExistsNeo4jManualTest, conditionalExistsSubqueries) {
    GTEST_SKIP() << "A conditional subquery body, WHEN ... THEN { } ELSE { }, has no clause "
                    "in the grammar: the parser stops at the WHEN";

    expectRows("MATCH (n:Person) "
               "WHERE EXISTS { "
               "  WHEN n.age > 35 THEN { "
               "    MATCH (n)-[:HAS_DOG]->(:Dog) "
               "    RETURN n AS petOwner "
               "  } "
               "  ELSE { "
               "    MATCH (n)-[:HAS_CAT]->(:Cat) "
               "    RETURN n AS petOwner "
               "  } "
               "} "
               "RETURN n.name AS name, "
               "       n.age AS age",
               {{"Andy", "36"}, {"Timothy", "25"}});
}

TEST_F(ExistsNeo4jManualTest, nestingExistsSubqueries) {
    expectRows("MATCH (person:Person) "
               "WHERE EXISTS { "
               "  MATCH (person)-[:HAS_DOG]->(dog:Dog) "
               "  WHERE EXISTS { "
               "    MATCH (dog)-[:HAS_TOY]->(toy:Toy) "
               "    WHERE toy.name = 'Banana' "
               "  } "
               "} "
               "RETURN person.name AS name",
               {{"Peter"}});
}

TEST_F(ExistsNeo4jManualTest, existsSubqueryOutsideOfAWhereClause) {
    expectRows("MATCH (person:Person) "
               "RETURN person.name AS name, EXISTS { "
               "  MATCH (person)-[:HAS_DOG]->(:Dog) "
               "} AS hasDog",
               {{"Andy", "true"}, {"Timothy", "false"}, {"Peter", "true"}});
}

TEST_F(ExistsNeo4jManualTest, existsSubqueryWithAUnion) {
    GTEST_SKIP() << "UNION inside a subquery body is rejected for every subquery, a CALL's "
                    "as much as an EXISTS's";

    expectRows("MATCH (person:Person) "
               "RETURN "
               "    person.name AS name, "
               "    EXISTS { "
               "        MATCH (person)-[:HAS_DOG]->(:Dog) "
               "        UNION "
               "        MATCH (person)-[:HAS_CAT]->(:Cat) "
               "    } AS hasPet",
               {{"Andy", "true"}, {"Timothy", "true"}, {"Peter", "true"}});
}

TEST_F(ExistsNeo4jManualTest, existsSubqueryWithWith) {
    expectRows("MATCH (person:Person) "
               "WHERE EXISTS { "
               "    WITH \"Ozzy\" AS dogName "
               "    MATCH (person)-[:HAS_DOG]->(d:Dog) "
               "    WHERE d.name = dogName "
               "} "
               "RETURN person.name AS name",
               {{"Peter"}});
}

TEST_F(ExistsNeo4jManualTest, existsSubqueryWithReturn) {
    expectRows("MATCH (person:Person) "
               "WHERE EXISTS { "
               "    MATCH (person)-[:HAS_DOG]->(:Dog) "
               "    RETURN person.name "
               "} "
               "RETURN person.name AS name",
               {{"Andy"}, {"Peter"}});
}
