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

// Every example of the Neo4j manual's COUNT subqueries page, with the graph that page
// builds and the rows it says each query answers:
// https://neo4j.com/docs/cypher-manual/current/subqueries/count/
class CountNeo4jManualTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

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
                              &_env->getMem(),
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

TEST_F(CountNeo4jManualTest, simpleCountSubquery) {
    expectRows("MATCH (person:Person) "
               "WHERE COUNT { (person)-[:HAS_DOG]->(:Dog) } > 1 "
               "RETURN person.name AS name",
               {{"Peter"}});
}

TEST_F(CountNeo4jManualTest, countSubqueryWithWhereClause) {
    expectRows("MATCH (person:Person) "
               "WHERE COUNT { "
               "  (person)-[:HAS_DOG]->(dog:Dog) "
               "  WHERE person.name = dog.name "
               "} = 1 "
               "RETURN person.name AS name",
               {{"Andy"}});
}

TEST_F(CountNeo4jManualTest, countSubqueryWithAUnion) {
    expectRows("MATCH (person:Person) "
               "RETURN "
               "    person.name AS name, "
               "    COUNT { "
               "        MATCH (person)-[:HAS_DOG]->(dog:Dog) "
               "        RETURN dog.name AS petName "
               "        UNION "
               "        MATCH (person)-[:HAS_CAT]->(cat:Cat) "
               "        RETURN cat.name AS petName "
               "    } AS numPets",
               {{"Andy", "1"}, {"Timothy", "1"}, {"Peter", "2"}});
}

TEST_F(CountNeo4jManualTest, countSubqueryWithWith) {
    expectRows("MATCH (person:Person) "
               "WHERE COUNT { "
               "    WITH \"Ozzy\" AS dogName "
               "    MATCH (person)-[:HAS_DOG]->(d:Dog) "
               "    WHERE d.name = dogName "
               "} = 1 "
               "RETURN person.name AS name",
               {{"Peter"}});
}

TEST_F(CountNeo4jManualTest, usingCountInReturn) {
    expectRows("MATCH (person:Person) "
               "RETURN person.name, COUNT { (person)-[:HAS_DOG]->(:Dog) } as howManyDogs",
               {{"Andy", "1"}, {"Timothy", "0"}, {"Peter", "2"}});
}

TEST_F(CountNeo4jManualTest, usingCountInSet) {
    expectWriteRows("MATCH (person:Person) WHERE person.name =\"Andy\" "
                    "SET person.howManyDogs = COUNT { (person)-[:HAS_DOG]->(:Dog) } "
                    "RETURN person.howManyDogs as howManyDogs",
                    {{"1"}});
}

TEST_F(CountNeo4jManualTest, usingCountInCase) {
    expectRows("MATCH (person:Person) "
               "RETURN "
               "   CASE "
               "     WHEN COUNT { (person)-[:HAS_DOG]->(:Dog) } > 1 THEN \"Doglover \" + person.name "
               "     ELSE person.name "
               "   END AS result",
               {{"Andy"}, {"Timothy"}, {"Doglover Peter"}});
}

TEST_F(CountNeo4jManualTest, usingCountAsAGroupingKey) {
    expectRowsInOrder("MATCH (person:Person) "
                      "RETURN COUNT { (person)-[:HAS_DOG]->(:Dog) } AS numDogs, "
                      "       avg(person.age) AS averageAge "
                      " ORDER BY numDogs",
                      {{"0", "25.000000"}, {"1", "36.000000"}, {"2", "35.000000"}});
}

TEST_F(CountNeo4jManualTest, countSubqueryWithReturn) {
    expectRows("MATCH (person:Person) "
               "WHERE COUNT { "
               "    MATCH (person)-[:HAS_DOG]->(:Dog) "
               "    RETURN person.name "
               "} = 1 "
               "RETURN person.name AS name",
               {{"Andy"}});
}
