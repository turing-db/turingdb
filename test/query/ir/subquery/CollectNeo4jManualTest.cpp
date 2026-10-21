#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <string_view>

#include "NLOutputSink.h"
#include "QueryConfig.h"
#include "QueryInterpreterV3.h"
#include "QueryStatus.h"

#include "Graph.h"
#include "JobSystem.h"
#include "SystemAccessor.h"
#include "SystemManager.h"
#include "TuringDB.h"
#include "versioning/Change.h"
#include "versioning/ChangeID.h"
#include "versioning/CommitHash.h"
#include "writers/GraphWriter.h"

#include "IRTestRows.h"
#include "TuringTest.h"
#include "TuringTestEnv.h"

using namespace db;
using namespace turing::test;

// Every example of the Neo4j manual's COLLECT subqueries page, with the graph that page
// builds and the rows it says each query answers:
// https://neo4j.com/docs/cypher-manual/current/subqueries/collect/
class CollectNeo4jManualTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager(), &_env->getMem(), &_env->getCompilerContext());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

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

    void runQuery(std::string_view query, const ChangeID& changeID, RowSink& sink) {
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              changeID,
                              &sink);

        ASSERT_TRUE(status.isOk()) << "query: " << query << "\nerror: " << status.getError();
    }

    void expectRows(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, ChangeID::head(), sink);

        Rows actual;
        sink.sortedRows(actual);

        Rows sortedExpected = expected;
        std::sort(sortedExpected.begin(), sortedExpected.end());

        std::string actualText;
        describeRows(actual, actualText);

        EXPECT_EQ(actual, sortedExpected) << "query: " << query << "\ngot:\n" << actualText;
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        runQuery(query, ChangeID::head(), sink);

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;
    }

    // Runs a writing query in a change of its own, then submits the change
    void expectWriteRows(std::string_view query, const Rows& expected) {
        ChangeID changeID;
        {
            SystemAccessor system = _env->getSystemManager().accessUnique();
            const auto change = system.newChange(_graphName);
            ASSERT_TRUE(change);

            changeID = change.value()->id();
        }

        RowSink sink;
        runQuery(query, changeID, sink);

        std::string actualText;
        describeRows(sink.rows(), actualText);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\ngot:\n" << actualText;

        const QueryState submitState(_graphName,
                                     &_env->getMem(),
                                     &_env->getCompilerContext(),
                                     &_queryConfig,
                                     nullptr,
                                     CommitHash::head(),
                                     changeID);
        const QueryStatus status = _env->getDB().query("CHANGE SUBMIT", submitState);
        ASSERT_TRUE(status.isOk()) << "CHANGE SUBMIT failed";
    }

    const std::string _graphName = "neo4jmanual";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
    QueryConfig _queryConfig;
};

TEST_F(CollectNeo4jManualTest, simpleCollectSubquery) {
    expectRows("MATCH (person:Person) "
               "WHERE 'Ozzy' IN COLLECT { MATCH (person)-[:HAS_DOG]->(dog:Dog) RETURN dog.name } "
               "RETURN person.name AS name",
               {{"Peter"}});
}

TEST_F(CollectNeo4jManualTest, collectSubqueryWithWhereClause) {
    expectRows("MATCH (person:Person) "
               "RETURN person.name as name, COLLECT { "
               "  MATCH (person)-[r:HAS_DOG]->(dog:Dog) "
               "  WHERE r.since > 2017 "
               "  RETURN dog.name "
               "} as youngDogs",
               {{"Andy", "[]"}, {"Timothy", "[]"}, {"Peter", "[Ozzy]"}});
}

TEST_F(CollectNeo4jManualTest, conditionalCollectSubquery) {
    GTEST_SKIP() << "The grammar has no conditional WHEN body in a COLLECT subquery";

    expectRows("MATCH (n:Person) "
               "RETURN n.name AS name, "
               "       COLLECT { "
               "         WHEN exists((n)-[:HAS_DOG]->(:Dog)) THEN { "
               "          RETURN 'Dog owner' AS petStatus "
               "         } "
               "         ELSE { "
               "          RETURN 'Cat owner' AS petStatus "
               "         } "
               "       } AS petStatus",
               {{"Andy", "[Dog owner]"}, {"Timothy", "[Cat owner]"}, {"Peter", "[Dog owner]"}});
}

// The manual lists Peter's pets as [Ozzy, Fido]: a UNION fixes no order, and this engine
// walks Peter's HAS_DOG edges in the order they were created
TEST_F(CollectNeo4jManualTest, collectSubqueryWithAUnion) {
    expectRows("MATCH (person:Person) "
               "RETURN "
               "    person.name AS name, "
               "    COLLECT { "
               "        MATCH (person)-[:HAS_DOG]->(dog:Dog) "
               "        RETURN dog.name AS petName "
               "        UNION "
               "        MATCH (person)-[:HAS_CAT]->(cat:Cat) "
               "        RETURN cat.name AS petName "
               "    } AS petNames",
               {{"Andy", "[Andy]"}, {"Timothy", "[Mittens]"}, {"Peter", "[Fido, Ozzy]"}});
}

TEST_F(CollectNeo4jManualTest, collectSubqueryWithWith) {
    expectRows("MATCH (person:Person) "
               "RETURN person.name AS name, COLLECT { "
               "    WITH 2018 AS yearOfTheDog "
               "    MATCH (person)-[r:HAS_DOG]->(d:Dog) "
               "    WHERE r.since = yearOfTheDog "
               "    RETURN d.name "
               "} as dogsOfTheYear",
               {{"Andy", "[]"}, {"Timothy", "[]"}, {"Peter", "[Ozzy]"}});
}

TEST_F(CollectNeo4jManualTest, usingCollectInReturn) {
    expectRows("MATCH (person:Person) "
               "RETURN person.name, "
               "       COLLECT { "
               "            MATCH (person)-[:HAS_DOG]->(d:Dog) "
               "            MATCH (d)-[:HAS_TOY]->(t:Toy) "
               "            RETURN t.name "
               "       } as toyNames",
               {{"Andy", "[]"}, {"Timothy", "[]"}, {"Peter", "[Banana]"}});
}

TEST_F(CollectNeo4jManualTest, usingCollectInSet) {
    expectWriteRows("MATCH (person:Person) WHERE person.name = \"Peter\" "
                    "SET person.dogNames = COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name } "
                    "RETURN person.dogNames as dogNames",
                    {{"[Fido, Ozzy]"}});
}

TEST_F(CollectNeo4jManualTest, usingCollectInCase) {
    expectRows("MATCH (person:Person) "
               "RETURN "
               "   CASE "
               "     WHEN COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name } = []  THEN \"No Dogs \" + person.name "
               "     ELSE person.name "
               "   END AS result",
               {{"Andy"}, {"No Dogs Timothy"}, {"Peter"}});
}

TEST_F(CollectNeo4jManualTest, usingCollectAsAGroupingKey) {
    expectRowsInOrder("MATCH (person:Person) "
                      "RETURN COLLECT { MATCH (person)-[:HAS_DOG]->(d:Dog) RETURN d.name } AS dogNames, "
                      "       avg(person.age) AS averageAge "
                      " ORDER BY dogNames",
                      {{"[]", "25.000000"}, {"[Andy]", "36.000000"}, {"[Fido, Ozzy]", "35.000000"}});
}

// The manual lists [Pete, Tim]: collect() fixes no order, and the scan meets Timothy first
TEST_F(CollectNeo4jManualTest, theCollectFunctionDropsNulls) {
    expectRows("MATCH (p:Person) RETURN collect(p.nickname) AS names", {{"[Tim, Pete]"}});
}

TEST_F(CollectNeo4jManualTest, aCollectSubqueryKeepsNulls) {
    expectRows("RETURN COLLECT { "
               "        MATCH (p:Person) "
               "        RETURN p.nickname ORDER BY p.nickname "
               "      } AS names",
               {{"[Pete, Tim, null]"}});
}

TEST_F(CollectNeo4jManualTest, aCollectSubqueryFiltersOutNulls) {
    expectRows("RETURN COLLECT { "
               "        MATCH (p:Person) "
               "        WHERE p.nickname IS NOT NULL "
               "        RETURN p.nickname ORDER BY p.nickname "
               "      } AS names",
               {{"[Pete, Tim]"}});
}
