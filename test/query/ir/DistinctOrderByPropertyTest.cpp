#include <gtest/gtest.h>

#include <stdint.h>

#include <memory>
#include <string>
#include <string_view>

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

// ORDER BY over a property of a variable a DISTINCT projects. The dedup keeps one row per
// distinct (c, p) pair, and c.name is one value per pair, so the key is read off the
// deduped column rather than the rows the match produced.
class DistinctOrderByPropertyTest : public TuringTest {
protected:
    void initialize() override {
        _env = TuringTestEnv::create(fs::Path {_outDir} / "turing");
        _interpreter = std::make_unique<QueryInterpreterV3>(&_env->getSystemManager());

        SystemAccessor system = _env->getSystemManager().accessUnique();
        buildGraph(system.createGraph(_graphName));
    }

    // Five Persons over three Cities. Person 2 is located in Antwerp over two edges, so the
    // match produces its pair twice and the answer depends on the dedup. Person 7 also
    // works in Ghent, which is the one edge of a second type.
    void buildGraph(Graph* graph) {
        JobSystem jobSystem;
        jobSystem.init();

        GraphWriter writer(graph, &jobSystem);

        const auto addCity = [&](std::string_view name) {
            const NodeID city = writer.addNode({"City"});
            writer.addNodeProperty<types::String>(city, "name", std::string_view(name));
            return city;
        };

        const NodeID antwerp = addCity("Antwerp");
        const NodeID brussels = addCity("Brussels");
        const NodeID ghent = addCity("Ghent");

        const auto addEdge = [&](std::string_view type, NodeID person, NodeID city, std::string_view name) {
            const EdgeRecord edge = writer.addEdge(std::string(type), person, city);
            writer.addEdgeProperty<types::String>(edge, "name", std::string_view(name));
        };

        const auto addPerson = [&](int64_t id, NodeID city, std::string_view edgeName) {
            const NodeID person = writer.addNode({"Person"});
            writer.addNodeProperty<types::Int64>(person, "id", std::move(id));
            addEdge("IS_LOCATED_IN", person, city, edgeName);
            return person;
        };

        addPerson(10, antwerp, "a-ten-antwerp");
        const NodeID second = addPerson(2, antwerp, "b-two-antwerp");
        const NodeID seventh = addPerson(7, brussels, "c-seven-brussels");
        addPerson(3, antwerp, "d-three-antwerp");
        addPerson(5, ghent, "e-five-ghent");

        addEdge("IS_LOCATED_IN", second, antwerp, "f-two-antwerp-again");
        addEdge("WORKS_IN", seventh, ghent, "g-seven-ghent");

        writer.submit();
        jobSystem.terminate();
    }

    void expectRowsInOrder(std::string_view query, const Rows& expected) {
        RowSink sink;
        QueryStatus status;
        _interpreter->execute(status,
                              query,
                              _graphName,
                              CommitHash::head(),
                              ChangeID::head(),
                              &_env->getMem(),
                              &sink);

        ASSERT_TRUE(status.isOk()) << status.getError();

        std::string description;
        describeRows(sink.rows(), description);

        EXPECT_EQ(sink.rows(), expected) << "query: " << query << "\nactual rows:\n" << description;
    }

    const std::string _graphName = "cities";
    std::unique_ptr<TuringTestEnv> _env;
    std::unique_ptr<QueryInterpreterV3> _interpreter;
};

// The three lowest Person ids of the first City by name. Charged to the six rows the match
// produced instead of the five the dedup left, the answer would repeat Antwerp 2.
TEST_F(DistinctOrderByPropertyTest, ordersDistinctPairsByTheirProperties) {
    expectRowsInOrder("MATCH (c:City)<-[:IS_LOCATED_IN]-(p:Person) "
                      "WITH DISTINCT c, p ORDER BY c.name ASC, p.id ASC "
                      "RETURN c.name, p.id LIMIT 3",
                      {{"Antwerp", "2"}, {"Antwerp", "3"}, {"Antwerp", "10"}});
}

// The same query without the cut: five pairs, not the six the match produced. The id order
// is the integer one, so 10 comes after 3.
TEST_F(DistinctOrderByPropertyTest, dedupsThePairsTheMatchRepeats) {
    expectRowsInOrder("MATCH (c:City)<-[:IS_LOCATED_IN]-(p:Person) "
                      "WITH DISTINCT c, p ORDER BY c.name ASC, p.id ASC "
                      "RETURN c.name, p.id",
                      {{"Antwerp", "2"},
                       {"Antwerp", "3"},
                       {"Antwerp", "10"},
                       {"Brussels", "7"},
                       {"Ghent", "5"}});
}

// The key read over the deduped column descending, which is the same five pairs reversed
TEST_F(DistinctOrderByPropertyTest, ordersDistinctPairsDescending) {
    expectRowsInOrder("MATCH (c:City)<-[:IS_LOCATED_IN]-(p:Person) "
                      "WITH DISTINCT c, p ORDER BY c.name DESC, p.id DESC "
                      "RETURN c.name, p.id",
                      {{"Ghent", "5"},
                       {"Brussels", "7"},
                       {"Antwerp", "10"},
                       {"Antwerp", "3"},
                       {"Antwerp", "2"}});
}

// A key computing over a property of a projected variable, not the property alone: p.id + 1
// is determined by p exactly as p.id is, and is read off the deduped column too.
TEST_F(DistinctOrderByPropertyTest, ordersDistinctPairsByAnExpressionOverAProperty) {
    expectRowsInOrder("MATCH (c:City)<-[:IS_LOCATED_IN]-(p:Person) "
                      "WITH DISTINCT c, p ORDER BY c.name ASC, p.id + 1 DESC "
                      "RETURN c.name, p.id",
                      {{"Antwerp", "10"},
                       {"Antwerp", "3"},
                       {"Antwerp", "2"},
                       {"Brussels", "7"},
                       {"Ghent", "5"}});
}

// One variable rather than a pair, and the cut charged to the sorted distinct rows: the
// three lowest Person ids of the graph.
TEST_F(DistinctOrderByPropertyTest, ordersASingleDistinctVariableByItsProperty) {
    expectRowsInOrder("MATCH (c:City)<-[:IS_LOCATED_IN]-(p:Person) "
                      "WITH DISTINCT p ORDER BY p.id ASC LIMIT 3 "
                      "RETURN p.id",
                      {{"2"}, {"3"}, {"5"}});
}

// The key tests the type of an edge the dedup kept. The self-join repeats every edge of a
// Person who has two, so the seven distinct edges come from eleven matched rows: read over
// those instead, the type would not line up with the edge it belongs to.
TEST_F(DistinctOrderByPropertyTest, ordersDistinctEdgesByTheirType) {
    expectRowsInOrder("MATCH (p:Person)-[e]->(c:City), (p)-->(c2:City) "
                      "WITH DISTINCT e ORDER BY e:WORKS_IN DESC, e.name ASC "
                      "RETURN e.name",
                      {{"g-seven-ghent"},
                       {"a-ten-antwerp"},
                       {"b-two-antwerp"},
                       {"c-seven-brussels"},
                       {"d-three-antwerp"},
                       {"e-five-ghent"},
                       {"f-two-antwerp-again"}});
}

int main(int argc, char** argv) {
    return turing::test::turingTestMain(argc, argv);
}
